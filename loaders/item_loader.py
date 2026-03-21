"""
loaders/item_loader.py

Role in pipeline: Loads validated F4101 item records into Odoo
product.template via XML-RPC.

Mirrors OdooLoader design for res.partner:
    - Atomic batch: stops on first failure, remaining marked NOT_PROCESSED
    - Idempotent: searches by default_code (ITM) before every create
    - Transaction log: uses entity_type='item', entity_id=ITM
    - Restart safety: already-loaded ITMs are skipped on re-run

Key difference from OdooLoader:
    product.template has Many2one UOM fields (uom_id, uom_po_id) that
    require integer Odoo IDs, not strings. The UomRegistry resolves
    JDE UOM codes to integer IDs at payload build time.

    res.partner fields are mostly direct values — no ID resolution needed.
"""

import xmlrpc.client
import sqlite3
import uuid
import os
from datetime import datetime
from dataclasses import dataclass, field
from loaders.odoo_loader import LoadStatus, RecordResult, LoadResult
from loaders.uom_registry import UomRegistry
from config.settings import get_settings
from utils.logger import get_logger

logger = get_logger(__name__)

# Odoo product.template fields accepted by the XML-RPC create call.
# Internal pipeline keys (_jde_itm, _jde_stkt, _jde_uom1, _jde_uom2)
# are excluded — Odoo does not recognize them.
# uom_id and uom_po_id are handled separately — they need integer IDs
# resolved from the UomRegistry, not the raw JDE string codes.
ODOO_PRODUCT_FIELDS = [
    "name", "description", "type",
    "sale_ok", "purchase_ok",
    "list_price",
]


class ItemLoader:
    """
    Loads valid transformed F4101 records into Odoo product.template via XML-RPC.

    Atomic batch strategy — all records load or the batch stops cleanly.
    Idempotent — safe to run multiple times without creating duplicates.
    Uses the same transaction log as OdooLoader with entity_type='item'.
    """

    def __init__(
        self,
        uom_registry: UomRegistry,
        db_path: str = "logs/transaction_log.db",
    ):
        """
        Initialize ItemLoader — connect to Odoo, prepare transaction log,
        preload existing product default_codes for idempotency check.

        Args:
            uom_registry (UomRegistry): Frozen UOM registry shared with validator.
                                        Used to resolve JDE UOM codes to Odoo IDs.
            db_path (str): Path to SQLite transaction log database.
        """
        self.settings     = get_settings()
        self.db_path      = db_path
        self.uom_registry = uom_registry
        self.batch_id     = str(uuid.uuid4())

        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._ensure_transaction_log()
        self._connect()

        logger.info(f"ItemLoader initialized | batch_id: {self.batch_id}")

    def _connect(self):
        """
        Authenticate with Odoo and store session UID and models proxy.
        Same pattern as OdooLoader — two XML-RPC endpoints for security.
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
                    f"Odoo authentication failed for {self.settings.odoo_username}"
                )
            self.models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")
            logger.info(
                f"Odoo connected | UID: {self.uid} | "
                f"DB: {db} | URL: {url}"
            )
        except Exception as e:
            logger.error(f"Odoo connection failed: {e}")
            raise

    def _ensure_transaction_log(self):
        """
        Create or migrate the transaction log table.
        Runs the same ALTER TABLE migration as OdooLoader to add
        entity_type and entity_id columns if they don't exist yet.
        Safe to run on any existing database state.
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS migration_log (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id       TEXT    NOT NULL,
                    attempt_number INTEGER NOT NULL DEFAULT 1,
                    an8            INTEGER,
                    entity_type    TEXT    NOT NULL DEFAULT 'customer',
                    entity_id      TEXT,
                    run_at         TEXT    NOT NULL,
                    status         TEXT    NOT NULL,
                    odoo_id        INTEGER,
                    error          TEXT,
                    record_name    TEXT
                )
            """)

            # Add columns if missing — catches databases created before
            # the entity_type/entity_id migration was introduced
            for column, definition in [
                ("entity_type", "TEXT NOT NULL DEFAULT 'customer'"),
                ("entity_id",   "TEXT"),
            ]:
                try:
                    conn.execute(
                        f"ALTER TABLE migration_log ADD COLUMN {column} {definition}"
                    )
                except Exception:
                    pass  # Column already exists — safe to ignore

            # Backfill entity_id from an8 for existing customer records
            conn.execute("""
                UPDATE migration_log
                SET entity_id   = CAST(an8 AS TEXT),
                    entity_type = 'customer'
                WHERE entity_id IS NULL AND an8 IS NOT NULL
            """)

            conn.commit()
        logger.info(f"Transaction log ready | path: {self.db_path}")

    def _get_loaded_itms(self) -> set[str]:
        """
        Return ITM values already successfully loaded in any previous run.
        Queries migration_log where entity_type='item' and status='LOADED'.

        Returns:
            set[str]: ITM values (as strings) already in the transaction log.
        """
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT entity_id FROM migration_log
                WHERE entity_type = 'item' AND status = ?
                """,
                (LoadStatus.LOADED.value,)
            ).fetchall()
        return {row[0] for row in rows}

    def _get_attempt_number(self, itm: str) -> int:
        """Return the next attempt number for this ITM."""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT MAX(attempt_number) FROM migration_log
                WHERE entity_type = 'item' AND entity_id = ?
                """,
                (itm,)
            ).fetchone()
        return (row[0] or 0) + 1

    def _log_record(
        self,
        itm: str,
        status: LoadStatus,
        record_name: str = None,
        odoo_id: int = None,
        error: str = None,
    ):
        """Write an item record's status to the transaction log."""
        attempt = self._get_attempt_number(itm)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO migration_log
                    (batch_id, attempt_number, an8, entity_type, entity_id,
                    run_at, status, odoo_id, error, record_name)
                VALUES (?, ?, ?, 'item', ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.batch_id,
                    attempt,
                    int(itm) if str(itm).isdigit() else 0,  # use ITM as an8 for uniqueness
                    str(itm),
                    datetime.now().isoformat(),
                    status.value,
                    odoo_id,
                    error,
                    record_name,
                )
            )
            conn.commit()

    def _product_exists_in_odoo(self, itm: int) -> int | None:
        """
        Search Odoo for an existing product with this ITM as default_code.
        Idempotent protection — detects products created in previous runs
        or created outside this pipeline.

        Args:
            itm (int): JDE Item Number

        Returns:
            int | None: Odoo product.template ID if found, None otherwise.
        """
        result = self.models.execute_kw(
            self.settings.odoo_db,
            self.uid,
            self.settings.odoo_password,
            "product.template",
            "search",
            [[["default_code", "=", str(itm)]]],
        )
        return result[0] if result else None

    def _build_product_payload(self, record: dict) -> dict:
        """
        Build a clean Odoo XML-RPC payload from a transformed item record.

        Resolves JDE UOM codes to Odoo integer IDs using the UomRegistry.
        This is the critical difference from res.partner — Many2one fields
        require integer IDs, not strings.
        Note: uom_po_id is not present in this Odoo instance — purchase_uom
        module not installed. uom_id handles both sales and purchase UOM.

        Replaces Python None with False — same pattern as OdooLoader.
        Sets default_code to ITM for idempotency detection on re-runs.

        Args:
            record (dict): Transformed record from ItemTransformer,
                           validated by ItemValidator.

        Returns:
            dict: Odoo-compatible payload for product.template create call.
        """
        payload = {}
        for field_name in ODOO_PRODUCT_FIELDS:
            value = record.get(field_name)
            payload[field_name] = value if value is not None else False

        # Store ITM as default_code — enables idempotent detection on restart.
        # Production upgrade: switch to x_jde_itm custom field via Odoo Studio.
        payload["default_code"] = str(record.get("_jde_itm", ""))

        # Resolve UOM codes to integer IDs — Many2one fields require integers.
        # Validator already confirmed these codes are resolvable.
        uom1_code = record.get("uom_id")    # still a JDE code at this point
        uom2_code = record.get("uom_po_id") # still a JDE code at this point

        if uom1_code:
            payload["uom_id"] = self.uom_registry.resolve(uom1_code).id
            # uom_po_id not present in this Odoo instance
            # purchase_uom module not installed — uom_id handles both sales and purchase

        return payload

    def load(self, valid_records: list[dict]) -> LoadResult:
        """
        Load valid item records into Odoo product.template atomically.

        Flow per record:
            1. Check transaction log — skip if already LOADED
            2. Check Odoo by default_code — skip if already exists
            3. Create in Odoo
            4. Log result
            5. On failure → mark remaining NOT_PROCESSED, stop batch

        Args:
            valid_records (list[dict]): Valid records from ItemValidator.

        Returns:
            LoadResult: Counts and per-record results for the report.
        """
        result = LoadResult(
            batch_id=self.batch_id,
            total=len(valid_records)
        )

        already_loaded_itms = self._get_loaded_itms()
        if already_loaded_itms:
            logger.info(
                f"Previous run detected | "
                f"skipping {len(already_loaded_itms)} already loaded items"
            )

        logger.info(
            f"Starting atomic batch load | "
            f"batch_id: {self.batch_id} | "
            f"total: {len(valid_records)}"
        )

        batch_stopped = False

        for record in valid_records:
            itm  = record.get("_jde_itm")
            name = record.get("name", "Unknown")
            itm_str = str(itm)

            # ── Skip: already loaded in a previous run ────────────────
            if itm_str in already_loaded_itms:
                result.skipped += 1
                result.records.append(
                    RecordResult(an8=itm, status=LoadStatus.SKIPPED)
                )
                logger.debug(f"SKIPPED (already loaded) | ITM={itm} | {name}")
                continue

            # ── Not processed: batch stopped by earlier failure ────────
            if batch_stopped:
                self._log_record(itm_str, LoadStatus.NOT_PROCESSED, name)
                result.not_processed += 1
                result.records.append(
                    RecordResult(an8=itm, status=LoadStatus.NOT_PROCESSED)
                )
                continue

            # ── Idempotent check: already exists in Odoo? ─────────────
            existing_odoo_id = self._product_exists_in_odoo(itm)
            if existing_odoo_id:
                self._log_record(
                    itm_str, LoadStatus.LOADED, name,
                    odoo_id=existing_odoo_id,
                    error="Already existed in Odoo — skipped creation"
                )
                result.loaded += 1
                result.records.append(
                    RecordResult(
                        an8=itm,
                        status=LoadStatus.LOADED,
                        odoo_id=existing_odoo_id
                    )
                )
                logger.info(
                    f"SKIPPED (exists in Odoo) | "
                    f"ITM={itm} | {name} | Odoo ID {existing_odoo_id}"
                )
                continue

            # ── Attempt Odoo create ───────────────────────────────────
            try:
                payload = self._build_product_payload(record)
                odoo_id = self.models.execute_kw(
                    self.settings.odoo_db,
                    self.uid,
                    self.settings.odoo_password,
                    "product.template",
                    "create",
                    [payload],
                )
                self._log_record(itm_str, LoadStatus.LOADED, name, odoo_id=odoo_id)
                result.loaded += 1
                result.records.append(
                    RecordResult(an8=itm, status=LoadStatus.LOADED, odoo_id=odoo_id)
                )
                logger.info(f"LOADED | ITM={itm} | {name} → Odoo ID {odoo_id}")

            except Exception as e:
                error_msg = str(e)
                self._log_record(itm_str, LoadStatus.FAILED, name, error=error_msg)
                result.failed += 1
                result.records.append(
                    RecordResult(an8=itm, status=LoadStatus.FAILED, error=error_msg)
                )
                logger.error(f"FAILED | ITM={itm} | {name} | {error_msg}")
                logger.warning(
                    f"Batch stopped — atomic integrity preserved. "
                    f"batch_id: {self.batch_id} | "
                    f"Fix ITM={itm} and re-run to continue."
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
    