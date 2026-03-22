"""
sync/sync_engine.py

Domain-agnostic sync orchestrator.

SyncEngine owns cross-cutting orchestration:
    - Watermark read from SyncLog
    - Delegates to pipeline for extract/transform/validate/load
    - NO_OP detection (zero records extracted)
    - Watermark persistence after successful run
    - Outcome classification
    - Run reporting trigger

SyncEngine never knows:
    - JDE table names or column prefixes
    - UomRegistry or any domain registry
    - Odoo model names (res.partner, product.template)
    - Business validation rules

These are all owned by the pipeline passed at construction time.
Adding a new table = create a new pipeline, zero changes to SyncEngine.
"""

import time
from dataclasses import dataclass, field
from enum import Enum
from pipelines.base_pipeline import BasePipeline
from sync.sync_log import SyncLog, SyncWatermark
from utils.logger import get_logger

logger = get_logger(__name__)


class SyncOutcome(Enum):
    """
    Classification of a sync run's final state.

    SUCCESS            — all extracted records loaded cleanly
    SUCCESS_WITH_SKIPS — loaded + some skipped (already existed)
    NO_OP              — zero records extracted since last watermark
    FAILED             — at least one record failed, no not_processed
    PARTIAL            — batch stopped mid-run (failed + not_processed)
    DRY_RUN            — dry run mode, no data written
    """
    SUCCESS            = "SUCCESS"
    SUCCESS_WITH_SKIPS = "SUCCESS_WITH_SKIPS"
    NO_OP              = "NO_OP"
    FAILED             = "FAILED"
    PARTIAL            = "PARTIAL"
    DRY_RUN            = "DRY_RUN"


@dataclass
class SyncResult:
    """
    Full observability for a single sync run.

    Attributes:
        outcome:           Final classification of this run
        table_name:        Pipeline table identifier e.g. 'customers'
        records_extracted: How many records came from the extractor
        records_valid:     How many passed validation
        records_failed:    How many failed validation
        records_loaded:    How many were written to Odoo
        records_skipped:   How many were skipped (already loaded)
        not_processed:     How many were not attempted (batch stopped)
        watermark_before:  Watermark at start of run
        watermark_after:   Watermark at end of run (None if not advanced)
        duration_seconds:  Wall clock time for the run
        exit_code:         0 = success/no-op, 1 = failure
        message:           Human-readable summary
    """
    outcome:           SyncOutcome       = SyncOutcome.NO_OP
    table_name:        str               = ""
    records_extracted: int               = 0
    records_valid:     int               = 0
    records_failed:    int               = 0
    records_loaded:    int               = 0
    records_skipped:   int               = 0
    not_processed:     int               = 0
    watermark_before:  SyncWatermark | None = None
    watermark_after:   SyncWatermark | None = None
    duration_seconds:  float             = 0.0
    exit_code:         int               = 0
    message:           str               = ""


class SyncEngine:
    """
    Domain-agnostic sync orchestrator.

    Accepts any pipeline that implements BasePipeline.
    Orchestrates watermark, extraction, transformation, validation,
    loading, outcome classification, and optional reporting.

    To add a new table: create a new Pipeline class.
    SyncEngine requires zero changes.
    """

    def __init__(
        self,
        pipeline: BasePipeline,
        dry_run: bool = False,
        generate_report: bool = False,
        limit: int | None = None,
        sync_log_path: str = "logs/transaction_log.db",
    ):
        """
        Initialize SyncEngine with a configured pipeline.

        Args:
            pipeline:        Configured pipeline (CustomerPipeline, ItemPipeline, etc.)
            dry_run:         If True, skip Odoo writes and report DRY_RUN outcome.
            generate_report: If True, generate Excel report after run.
            limit:           Optional record limit for debugging only.
            sync_log_path:   Path to SQLite sync log database.
        """
        self.pipeline        = pipeline
        self.dry_run         = dry_run
        self.generate_report = generate_report
        self.limit           = limit
        self.sync_log        = SyncLog(db_path=sync_log_path)

        logger.info(
            f"SyncEngine ready | "
            f"pipeline: {pipeline.describe()} | "
            f"dry_run: {dry_run}"
        )

    def run(self) -> SyncResult:
        """
        Execute one full sync cycle through the pipeline.

        Flow:
            1. Read watermark from SyncLog
            2. Extract records since watermark (delegates to pipeline)
            3. If zero records → NO_OP, return immediately
            4. Transform → Validate → Load
            5. Advance watermark on success
            6. Classify outcome
            7. Optional report generation

        Returns:
            SyncResult: Full observability for this run.
        """
        start_time = time.time()
        table_name = self.pipeline.table_name
        result     = SyncResult(table_name=table_name)

        # ── Step 1: Read watermark ────────────────────────────────────
        watermark = self.sync_log.get_watermark(table_name)
        result.watermark_before = watermark

        logger.info(
            f"SyncEngine starting | "
            f"pipeline: {self.pipeline.describe()} | "
            f"watermark: UPMJ={watermark.last_upmj} UPMT={watermark.last_upmt}"
        )

        # ── Step 2: Extract ───────────────────────────────────────────
        # Pipeline extractor owns incremental filtering.
        # SyncEngine passes watermark values — never interprets them.
        records = self.pipeline.extractor.extract(
            last_upmj=watermark.last_upmj,
            last_upmt=watermark.last_upmt,
        )

        if self.limit:
            records = records[:self.limit]
            logger.warning(
                f"LIMIT applied: {self.limit} records. "
                f"Watermark will reflect truncated batch — "
                f"use limit for debugging only, never in production sync."
            )

        result.records_extracted = len(records)

        # ── Step 3: NO_OP check ───────────────────────────────────────
        if len(records) == 0:
            result.outcome          = SyncOutcome.NO_OP
            result.exit_code        = 0
            result.duration_seconds = time.time() - start_time
            result.message          = (
                f"No new or updated records since last watermark "
                f"(UPMJ={watermark.last_upmj}, UPMT={watermark.last_upmt})"
            )
            logger.info(f"NO_OP | {result.message}")
            return result

        # ── Step 4: Transform ─────────────────────────────────────────
        transformed = self.pipeline.transformer.transform_batch(records)

        # ── Step 5: Validate ──────────────────────────────────────────
        valid_records, failed_records = self.pipeline.validator.validate_batch(
            transformed
        )
        result.records_valid  = len(valid_records)
        result.records_failed = len(failed_records)

        # ── Step 6: Load (or dry run) ─────────────────────────────────
        if self.dry_run:
            result.outcome          = SyncOutcome.DRY_RUN
            result.exit_code        = 0
            result.duration_seconds = time.time() - start_time
            result.message          = (
                f"Dry run — {len(valid_records)} valid, "
                f"{len(failed_records)} failed. No data written."
            )
            logger.info(f"DRY_RUN | {result.message}")
            return result

        load_result = self.pipeline.loader.load(valid_records)
        result.records_loaded  = load_result.loaded
        result.records_skipped = load_result.skipped
        result.not_processed   = load_result.not_processed

        # ── Step 7: Advance watermark ─────────────────────────────────
        # Delegate computation to pipeline — SyncEngine must not know
        # JDE field names like UPMJ or UPMT.
        # Only advance on non-failed outcomes — preserves re-run safety.
        new_watermark = self.pipeline.compute_watermark(records, watermark)

        if load_result.failed == 0:
            self.sync_log.update_watermark(
                table_name=table_name,
                last_upmj=new_watermark.last_upmj,
                last_upmt=new_watermark.last_upmt,
                records_synced=load_result.loaded,
            )
            result.watermark_after = new_watermark
            logger.info(
                f"Watermark advanced | "
                f"UPMJ={new_watermark.last_upmj} "
                f"UPMT={new_watermark.last_upmt}"
            )
        else:
            logger.warning(
                f"Watermark NOT advanced — "
                f"{load_result.failed} failure(s) in batch. "
                f"Re-run to retry failed records."
            )

        # ── Step 8: Classify outcome ──────────────────────────────────
        result.outcome      = self._classify_outcome(load_result)
        result.exit_code    = 1 if result.outcome in {
            SyncOutcome.FAILED, SyncOutcome.PARTIAL
        } else 0
        result.duration_seconds = time.time() - start_time
        result.message          = self._build_message(result)

        # ── Step 9: Optional report ───────────────────────────────────
        if self.generate_report:
            self._generate_report(valid_records, failed_records, load_result)

        logger.info(
            f"SyncEngine complete | "
            f"outcome: {result.outcome.value} | "
            f"loaded: {result.records_loaded} | "
            f"failed: {result.records_failed} | "
            f"duration: {result.duration_seconds:.2f}s"
        )

        return result

    # ── Private helpers ──────────────────────────────────────────────────────

    def _classify_outcome(self, load_result) -> SyncOutcome:
        """
        Classify the final outcome based on load result counts.

        PARTIAL  — failed > 0 AND not_processed > 0 (batch stopped mid-run)
        FAILED   — failed > 0 but all were attempted (no not_processed)
        SUCCESS_WITH_SKIPS — no failures, some skipped
        SUCCESS  — all loaded cleanly
        """
        if load_result.failed > 0 and load_result.not_processed > 0:
            return SyncOutcome.PARTIAL
        if load_result.failed > 0:
            return SyncOutcome.FAILED
        if load_result.skipped > 0:
            return SyncOutcome.SUCCESS_WITH_SKIPS
        return SyncOutcome.SUCCESS

    def _build_message(self, result: SyncResult) -> str:
        """Build a human-readable summary message for the result."""
        return (
            f"{result.outcome.value} | "
            f"extracted: {result.records_extracted} | "
            f"valid: {result.records_valid} | "
            f"loaded: {result.records_loaded} | "
            f"skipped: {result.records_skipped} | "
            f"failed: {result.records_failed}"
        )

    def _generate_report(self, valid_records, failed_records, load_result):
        """Trigger report generation — delegates to MigrationReport."""
        try:
            from reports.migration_report import MigrationReport
            report      = MigrationReport()
            report_path = report.generate(
                valid_records=valid_records,
                failed_records=failed_records,
                dry_run=False,
                source="sync",
                load_result=load_result,
            )
            logger.info(f"Report generated: {report_path}")
        except Exception as e:
            logger.warning(f"Report generation failed (non-fatal): {e}")
            