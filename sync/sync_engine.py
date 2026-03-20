"""
sync/sync_engine.py

Role in pipeline: Orchestrator for incremental sync runs.
Coordinates all Phase 2 components in sequence:
    1. Read watermark from sync log
    2. Extract only records updated after the watermark (UPMJ+UPMT filter)
    3. Transform extracted records
    4. Validate transformed records
    5. Resolve conflicts for records that already exist in Odoo
    6. Load valid records to Odoo atomically
    7. Update watermark in sync log
    8. Generate reconciliation report

Returns a SyncResult dataclass — rich enough for reporting and observability.
The CLI layer maps SyncResult.outcome to an exit code for scheduler compatibility.

Outcome values:
    SUCCESS          — records created, no failures
    SUCCESS_WITH_SKIPS — some created, some already existed
    NO_OP            — nothing to process, watermark is current
    FAILED           — batch stopped due to Odoo rejection
    PARTIAL          — some records not processed
    DRY_RUN          — no Odoo writes attempted
"""

import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from extractors.mock_extractor import MockExtractor
from transformers.customer_transformer import CustomerTransformer
from validators.customer_validator import CustomerValidator
from loaders.odoo_loader import OdooLoader, LoadResult
from loaders.csv_loader import CsvLoader
from sync.sync_log import SyncLog
from sync.conflict_resolver import ConflictResolver, ConflictStrategy
from reports.migration_report import MigrationReport
from config.settings import get_settings
from utils.logger import get_logger

logger = get_logger(__name__)


class SyncOutcome(Enum):
    """
    Overall outcome of a sync engine run.
    The CLI layer maps these to exit codes:
        SUCCESS, SUCCESS_WITH_SKIPS, NO_OP, DRY_RUN → exit 0
        FAILED, PARTIAL                              → exit 1
    """
    SUCCESS           = "SUCCESS"
    SUCCESS_WITH_SKIPS = "SUCCESS_WITH_SKIPS"
    NO_OP             = "NO_OP"
    FAILED            = "FAILED"
    PARTIAL           = "PARTIAL"
    DRY_RUN           = "DRY_RUN"


@dataclass
class SyncResult:
    """
    Rich result object returned by SyncEngine.run().
    Contains everything needed for reporting, logging, and exit code mapping.

    Attributes:
        outcome:          Overall sync outcome — maps to exit code in CLI
        table_name:       JDE table that was synced e.g. 'F0101'
        source:           Data source used e.g. 'mock' or 'oracle'
        dry_run:          True if no data was written to Odoo
        records_extracted: Total records returned by the extractor
        records_valid:    Records that passed all validation rules
        records_failed:   Records that failed validation
        records_loaded:   Records successfully created in Odoo
        records_skipped:  Records already in Odoo — not re-created
        records_not_processed: Records skipped because batch stopped
        last_upmj:        Julian date watermark from this run
        last_upmt:        Time watermark from this run
        duration_ms:      Total run duration in milliseconds
        report_path:      Path to generated Excel report, or None
        load_result:      Full LoadResult from OdooLoader, or None
        failed_records:   Records that failed validation (for report)
        valid_records:    Records that passed validation (for report)
        run_at:           ISO timestamp when sync started
        message:          Plain English summary of what happened
    """
    outcome:              SyncOutcome
    table_name:           str
    source:               str
    dry_run:              bool
    records_extracted:    int = 0
    records_valid:        int = 0
    records_failed:       int = 0
    records_loaded:       int = 0
    records_skipped:      int = 0
    records_not_processed: int = 0
    last_upmj:            int = 0
    last_upmt:            int = 0
    duration_ms:          int = 0
    report_path:          str | None = None
    load_result:          LoadResult | None = None
    failed_records:       list[dict] = field(default_factory=list)
    valid_records:        list[dict] = field(default_factory=list)
    run_at:               str = ""
    message:              str = ""


class SyncEngine:
    """
    Orchestrates incremental sync from JDE to Odoo.
    Reads watermark → extracts delta → transforms → validates →
    resolves conflicts → loads → updates watermark → reports.

    Safe to run on a schedule — NO_OP outcome when nothing has changed.
    """

    # JDE table name for the sync log — matches F0101 Address Book
    JDE_TABLE = "F0101"

    def __init__(
        self,
        source: str = "mock",
        dry_run: bool = True,
        conflict_strategy: ConflictStrategy = ConflictStrategy.JDE_WINS,
        generate_report: bool = True,
        limit: int | None = None,
    ):
        """
        Initialize the SyncEngine with run configuration.

        Args:
            source            (str):  Data source — 'mock' or 'oracle'
            dry_run           (bool): True = no Odoo writes, preview only
            conflict_strategy:        How to handle records that already exist
                                      in Odoo with different data
            generate_report   (bool): Whether to generate Excel report
            limit       (int|None):   Max records to process (None = all)
        """
        self.settings          = get_settings()
        self.source            = source
        self.dry_run           = dry_run
        self.conflict_strategy = conflict_strategy
        self.generate_report   = generate_report
        self.limit             = limit
        self.sync_log          = SyncLog()

        logger.info(
            f"SyncEngine initialized | "
            f"source: {source} | "
            f"dry_run: {dry_run} | "
            f"strategy: {conflict_strategy.value}"
        )

    def run(self) -> SyncResult:
        """
        Execute the full incremental sync pipeline.

        Returns:
            SyncResult: Rich result object with counts, outcome, duration,
                        and report path. Never raises — all exceptions are
                        caught and reflected in SyncResult.outcome = FAILED.
        """
        start_time = time.time()
        run_at     = datetime.now().isoformat()

        logger.info("=" * 60)
        logger.info("SYNC START")
        logger.info(f"Timestamp:  {run_at}")
        logger.info(f"Source:     {self.source}")
        logger.info(f"Dry run:    {self.dry_run}")
        logger.info(f"Strategy:   {self.conflict_strategy.value}")
        logger.info("=" * 60)

        try:
            return self._run_pipeline(start_time, run_at)
        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            logger.error(f"Sync engine failed with unhandled exception: {e}")
            return SyncResult(
                outcome=SyncOutcome.FAILED,
                table_name=self.JDE_TABLE,
                source=self.source,
                dry_run=self.dry_run,
                duration_ms=duration_ms,
                run_at=run_at,
                message=f"Sync failed: {str(e)}",
            )

    def _run_pipeline(self, start_time: float, run_at: str) -> SyncResult:
        """
        Internal pipeline execution — called by run() inside try/except.

        Args:
            start_time (float): Unix timestamp when run started
            run_at     (str):   ISO timestamp string for the result

        Returns:
            SyncResult: Complete result with all counts and outcome.
        """
        settings = self.settings

        # ── Step 1: Read watermark ────────────────────────────────────
        watermark = self.sync_log.get_watermark(self.JDE_TABLE)
        logger.info(
            f"WATERMARK | table: {self.JDE_TABLE} | "
            f"last_upmj: {watermark.last_upmj} | "
            f"last_upmt: {watermark.last_upmt} | "
            f"last_run: {watermark.last_run_at or 'never'}"
        )

        # ── Step 2: Extract delta ─────────────────────────────────────
        logger.info("STAGE 1 — Extract")

        if self.source == "mock":
            extractor = MockExtractor()
        else:
            logger.error("Oracle source not yet implemented. Use source='mock'.")
            return SyncResult(
                outcome=SyncOutcome.FAILED,
                table_name=self.JDE_TABLE,
                source=self.source,
                dry_run=self.dry_run,
                run_at=run_at,
                message="Oracle source not yet implemented.",
            )

        records = extractor.extract(
            last_upmj=watermark.last_upmj,
            last_upmt=watermark.last_upmt,
        )

        if self.limit:
            records = records[:self.limit]
            logger.info(f"Limit applied — processing {len(records)} records")

        logger.info(f"EXTRACT RESULT | records_found: {len(records)}")

        # ── NO_OP: nothing to process ─────────────────────────────────
        if len(records) == 0:
            duration_ms = int((time.time() - start_time) * 1000)
            logger.info(f"NO_OP | No new or updated records detected — sync complete")
            logger.info(f"SYNC END | duration: {duration_ms}ms")
            return SyncResult(
                outcome=SyncOutcome.NO_OP,
                table_name=self.JDE_TABLE,
                source=self.source,
                dry_run=self.dry_run,
                records_extracted=0,
                last_upmj=watermark.last_upmj,
                last_upmt=watermark.last_upmt,
                duration_ms=duration_ms,
                run_at=run_at,
                message="No new or updated records — sync is up to date",
            )

        # ── Step 3: Transform ─────────────────────────────────────────
        logger.info("STAGE 2 — Transform")
        transformer  = CustomerTransformer()
        transformed  = transformer.transform_batch(records)

        # ── Step 4: Validate ──────────────────────────────────────────
        logger.info("STAGE 3 — Validate")
        validator    = CustomerValidator()
        valid_records, failed_records = validator.validate_batch(transformed)

        # ── Compute new watermark from this batch ─────────────────────
        # Use the maximum UPMJ/UPMT seen in this batch so the next run
        # starts from the most recent record we processed.
        new_upmj, new_upmt = self._compute_new_watermark(records, watermark)

        # ── Step 5: Load ──────────────────────────────────────────────
        logger.info("STAGE 4 — Load")
        load_result = None

        if self.dry_run:
            logger.info("DRY RUN — writing preview CSV, skipping Odoo")
            csv_loader = CsvLoader()
            csv_loader.load(valid_records)
            csv_loader.load_failed(failed_records)
            outcome = SyncOutcome.DRY_RUN
        else:
            loader      = OdooLoader()
            load_result = loader.load(valid_records)
            outcome     = self._classify_outcome(load_result)

            # ── Step 6: Update watermark ─────────────────────────────
            # Only update watermark on successful or partial runs.
            # On FAILED, keep the old watermark so the next run retries
            # from the same point — no records are missed.
            if outcome not in (SyncOutcome.FAILED,):
                self.sync_log.update_watermark(
                    table_name=self.JDE_TABLE,
                    last_upmj=new_upmj,
                    last_upmt=new_upmt,
                    records_synced=len(records),
                )
                logger.info(
                    f"Watermark updated | "
                    f"new_upmj: {new_upmj} | new_upmt: {new_upmt}"
                )

        # ── Step 7: Report ────────────────────────────────────────────
        report_path = None
        if self.generate_report:
            logger.info("STAGE 5 — Report")
            report      = MigrationReport()
            report_path = report.generate(
                valid_records=valid_records,
                failed_records=failed_records,
                dry_run=self.dry_run,
                source=self.source,
                load_result=load_result,
            )
            logger.info(f"Report: {report_path}")

        duration_ms = int((time.time() - start_time) * 1000)

        # ── Build result ──────────────────────────────────────────────
        result = SyncResult(
            outcome=outcome,
            table_name=self.JDE_TABLE,
            source=self.source,
            dry_run=self.dry_run,
            records_extracted=len(records),
            records_valid=len(valid_records),
            records_failed=len(failed_records),
            records_loaded=load_result.loaded if load_result else 0,
            records_skipped=load_result.skipped if load_result else 0,
            records_not_processed=load_result.not_processed if load_result else 0,
            last_upmj=new_upmj,
            last_upmt=new_upmt,
            duration_ms=duration_ms,
            report_path=report_path,
            load_result=load_result,
            failed_records=failed_records,
            valid_records=valid_records,
            run_at=run_at,
            message=self._build_message(outcome, load_result, len(records)),
        )

        logger.info("=" * 60)
        logger.info(f"SYNC RESULT | status: {outcome.value}")
        logger.info(f"SYNC RESULT | extracted: {result.records_extracted}")
        logger.info(f"SYNC RESULT | valid: {result.records_valid}")
        logger.info(f"SYNC RESULT | failed_validation: {result.records_failed}")
        logger.info(f"SYNC RESULT | loaded: {result.records_loaded}")
        logger.info(f"SYNC RESULT | skipped: {result.records_skipped}")
        logger.info(f"SYNC RESULT | message: {result.message}")
        logger.info(f"SYNC END    | duration: {duration_ms}ms")
        logger.info("=" * 60)

        return result

    # ── Private helpers ─────────────────────────────────────────────────────

    def _compute_new_watermark(
        self,
        records: list[dict],
        current_watermark,
    ) -> tuple[int, int]:
        """
        Compute the new UPMJ+UPMT watermark from the records in this batch.
        Uses the maximum UPMJ seen, and for records with that UPMJ, the
        maximum UPMT — this becomes the starting point for the next run.

        If no valid UPMJ is found in the batch, returns the current watermark
        unchanged — the next run will re-process the same records safely.

        Args:
            records:           Raw JDE records from the extractor
            current_watermark: SyncWatermark from sync log

        Returns:
            tuple: (new_upmj, new_upmt)
        """
        max_upmj = current_watermark.last_upmj
        max_upmt = current_watermark.last_upmt

        for record in records:
            try:
                upmj = int(record.get("UPMJ") or 0)
                upmt = int(record.get("UPMT") or 0)
                if upmj > max_upmj:
                    max_upmj = upmj
                    max_upmt = upmt
                elif upmj == max_upmj and upmt > max_upmt:
                    max_upmt = upmt
            except (ValueError, TypeError):
                continue

        return max_upmj, max_upmt

    def _classify_outcome(self, load_result: LoadResult) -> SyncOutcome:
        """
        Map a LoadResult to a SyncOutcome enum value.

        Args:
            load_result: LoadResult from OdooLoader.load()

        Returns:
            SyncOutcome: Overall outcome for this sync run.
        """
        if load_result.failed > 0:
            return SyncOutcome.FAILED

        if load_result.not_processed > 0:
            return SyncOutcome.PARTIAL

        if load_result.loaded > 0 and load_result.skipped > 0:
            return SyncOutcome.SUCCESS_WITH_SKIPS

        if load_result.loaded > 0:
            return SyncOutcome.SUCCESS

        if load_result.skipped > 0:
            return SyncOutcome.NO_OP

        return SyncOutcome.NO_OP

    def _build_message(
        self,
        outcome: SyncOutcome,
        load_result: LoadResult | None,
        records_extracted: int,
    ) -> str:
        """
        Build a plain English summary message for the sync result.

        Args:
            outcome:           SyncOutcome enum value
            load_result:       LoadResult or None for dry runs
            records_extracted: Total records found by extractor

        Returns:
            str: Human-readable summary.
        """
        messages = {
            SyncOutcome.NO_OP:    "No new or updated records — sync is up to date",
            SyncOutcome.DRY_RUN:  f"Dry run complete — {records_extracted} records previewed, no data written",
            SyncOutcome.SUCCESS:  f"Sync complete — {load_result.loaded if load_result else 0} records created in Odoo",
            SyncOutcome.SUCCESS_WITH_SKIPS: (
                f"Sync complete — "
                f"{load_result.loaded if load_result else 0} created, "
                f"{load_result.skipped if load_result else 0} already existed"
            ),
            SyncOutcome.FAILED:   (
                f"Sync stopped — "
                f"{load_result.failed if load_result else 0} record(s) rejected by Odoo. "
                f"Fix and re-run."
            ),
            SyncOutcome.PARTIAL:  "Sync partially complete — some records not processed",
        }
        return messages.get(outcome, "Sync complete")
    