"""
loaders/odoo_loader.py

Role in pipeline: Live load stage — connects to Odoo via XML-RPC and
creates res.partner records from valid transformed records.

Implements atomic batch loading with full transaction integrity:
- Atomic stop-on-failure: if any record fails, batch stops immediately
- Idempotent protection: searches Odoo by AN8 ref before every create
- Batch metadata: every run has a unique batch_id for audit traceability
- Restart safety: LOADED records are skipped on re-run, no duplicates

Transaction log (SQLite) persists across runs at logs/transaction_log.db

Status values:
    PENDING       — queued, not yet attempted
    LOADED        — successfully created in Odoo
    FAILED        — Odoo rejected this record
    NOT_PROCESSED — batch stopped before this record was attempted
"""

import xmlrpc.client
import sqlite3
import uuid
import os
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
from config.settings import get_settings
from utils.logger import get_logger

logger = get_logger(__name__)


class LoadStatus(Enum):
    """
    Status values for each record in the transaction log.
    Enum prevents typo bugs — LoadStatus.LAODED raises AttributeError
    immediately. The string "LAODED" would silently pass and break
    status comparisons downstream.
    """
    PENDING       = "PENDING"
    LOADED        = "LOADED"
    SKIPPED       = "SKIPPED"
    FAILED        = "FAILED"
    NOT_PROCESSED = "NOT_PROCESSED"


@dataclass
class RecordResult:
    """
    Result for a single record after load attempt.

    Attributes:
        an8:        JDE Address Number — primary key for traceability
        status:     LoadStatus value
        odoo_id:    Odoo partner ID if LOADED, None otherwise
        error:      Error message if FAILED, None otherwise
    """
    an8: int
    status: LoadStatus
    odoo_id: int | None = None
    error: str | None = None


@dataclass
class LoadResult:
    """
    Aggregate result for the entire batch load operation.

    Attributes:
        batch_id:      Unique ID for this run — use to query transaction log
        total:         Total records in the valid list
        loaded:        Successfully created in Odoo
        failed:        Rejected by Odoo
        not_processed: Skipped because batch stopped early
        skipped:       Already loaded in a previous run
        records:       Per-record results for the reconciliation report
    """
    batch_id: str = ""
    total: int = 0
    loaded: int = 0
    failed: int = 0
    not_processed: int = 0
    skipped: int = 0
    records: list[RecordResult] = field(default_factory=list)


# Odoo res.partner fields accepted by the XML-RPC create call.
# Internal pipeline keys (_jde_an8, _jde_at1, state_code, country_code)
# are excluded — Odoo does not recognize them and will reject the call.
ODOO_PARTNER_FIELDS = [
    "name", "phone", "street", "street2", "city",
    "zip", "vat", "customer_rank", "is_company", "comment",
]


class OdooLoader:
    """
    Loads valid transformed records into Odoo res.partner via XML-RPC.

    Atomic batch strategy — all records load or the batch stops cleanly.
    Idempotent — safe to run multiple times without creating duplicates.
    Every run is assigned a unique batch_id for full audit traceability.
    """

    def __init__(self, db_path: str = "logs/transaction_log.db"):
        """
        Initialize OdooLoader — connect to Odoo and prepare transaction log.

        A new batch_id (UUID) is generated per instantiation. This means
        each pipeline run is independently traceable in the transaction log.

        Args:
            db_path (str): Path to SQLite transaction log database.
        """
        self.settings = get_settings()
        self.db_path = db_path

        # UUID4 = random UUID — unique per run, collision probability negligible
        # Used to group all log entries from this run for easy querying
        self.batch_id = str(uuid.uuid4())

        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_transaction_log()
        self._connect()

        logger.info(f"OdooLoader initialized | batch_id: {self.batch_id}")

    def _connect(self):
        """
        Authenticate with Odoo and store session UID and models proxy.

        Two XML-RPC endpoints — separated for security:
            /xmlrpc/2/common — public, handles authentication only.
                               No credentials needed to reach this endpoint.
            /xmlrpc/2/object — private, all data operations.
                               Requires uid + password on every single call.
        Separation ensures a bug in authentication cannot accidentally
        expose unauthenticated access to data operations.

        Raises:
            ConnectionError: If authentication fails.
        """
        try:
            url = self.settings.odoo_url
            db  = self.settings.odoo_db

            common   = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
            self.uid = common.authenticate(
                db,
                self.settings.odoo_username,
                self.settings.odoo_password,
                {}
            )

            if not self.uid:
                raise ConnectionError(
                    f"Odoo authentication failed for "
                    f"{self.settings.odoo_username}. "
                    f"Check ODOO_USERNAME and ODOO_PASSWORD in .env"
                )

            self.models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")
            logger.info(
                f"Odoo connected | UID: {self.uid} | "
                f"DB: {db} | URL: {url}"
            )

        except Exception as e:
            logger.error(f"Odoo connection failed: {e}")
            raise

    def _init_transaction_log(self):
        """
        Create the transaction log table if it does not exist.
        UNIQUE(batch_id, an8) prevents duplicate log entries per run.
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS migration_log (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id       TEXT    NOT NULL,
                    attempt_number INTEGER NOT NULL DEFAULT 1,
                    an8            INTEGER NOT NULL,
                    run_at         TEXT    NOT NULL,
                    status         TEXT    NOT NULL,
                    odoo_id        INTEGER,
                    error          TEXT,
                    record_name    TEXT,
                    UNIQUE(batch_id, an8)
                )
            """)
            conn.commit()
        logger.info(f"Transaction log ready | path: {self.db_path}")

    def _get_loaded_an8s(self) -> set[int]:
        """
        Return AN8 values already successfully loaded in any previous run.
        Used to skip records on restart — prevents Odoo duplicates.

        Returns:
            set[int]: AN8 values with status LOADED across all runs.
        """
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT an8 FROM migration_log WHERE status = ?",
                (LoadStatus.LOADED.value,)
            ).fetchall()
        return {row[0] for row in rows}

    def _get_attempt_number(self, an8: int) -> int:
        """
        Return the next attempt number for this AN8.
        Increments on each retry so audit trail shows full history.

        Args:
            an8 (int): JDE Address Number

        Returns:
            int: Next attempt number (1 for first attempt)
        """
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT MAX(attempt_number) FROM migration_log WHERE an8 = ?",
                (an8,)
            ).fetchone()
        return (row[0] or 0) + 1

    def _log_record(
        self,
        an8: int,
        status: LoadStatus,
        record_name: str = None,
        odoo_id: int = None,
        error: str = None,
    ):
        """
        Write a record's status to the transaction log.

        Args:
            an8:         JDE Address Number
            status:      LoadStatus enum value
            record_name: Customer name for human readability in log
            odoo_id:     Odoo partner ID if LOADED
            error:       Error message if FAILED
        """
        attempt = self._get_attempt_number(an8)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO migration_log
                    (batch_id, attempt_number, an8, run_at,
                     status, odoo_id, error, record_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.batch_id,
                    attempt,
                    an8,
                    datetime.now().isoformat(),
                    status.value,
                    odoo_id,
                    error,
                    record_name,
                )
            )
            conn.commit()

    def _partner_exists_in_odoo(self, an8: int) -> int | None:
        """
        Search Odoo for an existing partner with this AN8 in the ref field.
        Idempotent protection — we store AN8 as the external reference so
        we can detect records already created in a previous partial run.

        Args:
            an8 (int): JDE Address Number to search for.

        Returns:
            int | None: Odoo partner ID if found, None if not found.
        """
        result = self.models.execute_kw(
            self.settings.odoo_db,
            self.uid,
            self.settings.odoo_password,
            "res.partner",
            "search",
            # Search for partners where ref field equals the AN8 string
            [[["ref", "=", str(an8)]]],
        )
        return result[0] if result else None

    def _build_partner_payload(self, record: dict) -> dict:
        """
        Build a clean Odoo XML-RPC payload from a transformed record.

        Strips internal pipeline keys not recognized by Odoo.
        Replaces Python None with False — None cannot be serialized
        by xmlrpc.client (no XML-RPC equivalent), False is Odoo's
        standard null value for most field types.

        Also adds the AN8 as the ref field — this is the external ID
        that enables idempotent duplicate detection on re-runs.

        Args:
            record (dict): Transformed record from CustomerTransformer.

        Returns:
            dict: Odoo-compatible payload for res.partner create call.
        """
        payload = {}
        for field_name in ODOO_PARTNER_FIELDS:
            value = record.get(field_name)
            # None → False: XML-RPC cannot serialize Python None.
            # False is Odoo's null — accepted by all field types.
            payload[field_name] = value if value is not None else False

        # Store AN8 as external reference — enables idempotent detection
        # on restart. Search by ref before every create call.
        payload["ref"] = str(record.get("_jde_an8", ""))

        return payload

    def load(self, valid_records: list[dict]) -> LoadResult:
        """
        Load valid records into Odoo res.partner atomically.

        Flow per record:
            1. Check if already LOADED in transaction log → skip
            2. Check if already exists in Odoo by ref (AN8) → skip
            3. Create in Odoo
            4. Log result
            5. On failure → mark remaining NOT_PROCESSED, stop batch

        Args:
            valid_records (list[dict]): Valid records from CustomerValidator.

        Returns:
            LoadResult: Counts and per-record results for the report.
        """
        result = LoadResult(
            batch_id=self.batch_id,
            total=len(valid_records)
        )

        # Skip records already loaded in any previous run
        already_loaded_an8s = self._get_loaded_an8s()
        if already_loaded_an8s:
            logger.info(
                f"Previous run detected | "
                f"skipping {len(already_loaded_an8s)} already loaded records"
            )

        logger.info(
            f"Starting atomic batch load | "
            f"batch_id: {self.batch_id} | "
            f"total: {len(valid_records)}"
        )

        batch_stopped = False

        for record in valid_records:
            an8  = record.get("_jde_an8")
            name = record.get("name", "Unknown")

            # ── Skip: already loaded in a previous run ────────────────
            if an8 in already_loaded_an8s:
                result.skipped += 1
                result.records.append(
                    RecordResult(an8=an8, status=LoadStatus.SKIPPED)
                )
                logger.debug(f"SKIPPED (already loaded) | AN8={an8} | {name}")
                continue

            # ── Not processed: batch stopped by earlier failure ────────
            if batch_stopped:
                self._log_record(an8, LoadStatus.NOT_PROCESSED, name)
                result.not_processed += 1
                result.records.append(
                    RecordResult(an8=an8, status=LoadStatus.NOT_PROCESSED)
                )
                continue

            # ── Idempotent check: already exists in Odoo? ─────────────
            existing_odoo_id = self._partner_exists_in_odoo(an8)
            if existing_odoo_id:
                # Record exists in Odoo but not in our log —
                # probably created outside this pipeline.
                # Log as LOADED to prevent future re-creation attempts.
                self._log_record(
                    an8, LoadStatus.LOADED, name,
                    odoo_id=existing_odoo_id,
                    error="Already existed in Odoo — skipped creation"
                )
                result.loaded += 1
                result.records.append(
                    RecordResult(
                        an8=an8,
                        status=LoadStatus.LOADED,
                        odoo_id=existing_odoo_id
                    )
                )
                logger.info(
                    f"SKIPPED (exists in Odoo) | "
                    f"AN8={an8} | {name} | "
                    f"Odoo ID {existing_odoo_id}"
                )
                continue

            # ── Attempt Odoo create ───────────────────────────────────
            try:
                payload  = self._build_partner_payload(record)
                odoo_id  = self.models.execute_kw(
                    self.settings.odoo_db,
                    self.uid,
                    self.settings.odoo_password,
                    "res.partner",
                    "create",
                    [payload],
                )

                self._log_record(
                    an8, LoadStatus.LOADED, name, odoo_id=odoo_id
                )
                result.loaded += 1
                result.records.append(
                    RecordResult(
                        an8=an8,
                        status=LoadStatus.LOADED,
                        odoo_id=odoo_id
                    )
                )
                logger.info(
                    f"LOADED | AN8={an8} | {name} → Odoo ID {odoo_id}"
                )

            except Exception as e:
                error_msg = str(e)
                self._log_record(
                    an8, LoadStatus.FAILED, name, error=error_msg
                )
                result.failed += 1
                result.records.append(
                    RecordResult(
                        an8=an8,
                        status=LoadStatus.FAILED,
                        error=error_msg
                    )
                )
                logger.error(
                    f"FAILED | AN8={an8} | {name} | {error_msg}"
                )
                logger.warning(
                    f"Batch stopped — atomic integrity preserved. "
                    f"batch_id: {self.batch_id} | "
                    f"Fix AN8={an8} and re-run to continue."
                )
                batch_stopped = True

        logger.info(
            f"Batch complete | "
            f"loaded: {result.loaded} | "
            f"failed: {result.failed} | "
            f"not_processed: {result.not_processed} | "
            f"skipped: {result.skipped}"
        )
        return result
    