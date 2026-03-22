"""
pipelines/customer_pipeline.py

Customer migration pipeline — F0101 → Odoo res.partner.

Assembles the full customer stack:
    MockExtractor / JdeExtractor → CustomerTransformer →
    CustomerValidator → OdooLoader / CsvLoader

This pipeline knows JDE F0101 schema and Odoo res.partner structure.
SyncEngine knows neither.
"""

from pipelines.base_pipeline import BasePipeline
from extractors.mock_extractor import MockExtractor
from transformers.customer_transformer import CustomerTransformer
from validators.customer_validator import CustomerValidator
from loaders.odoo_loader import OdooLoader
from loaders.csv_loader import CsvLoader
from sync.sync_log import SyncWatermark
from config.settings import get_settings
from utils.logger import get_logger

logger = get_logger(__name__)


class CustomerPipeline(BasePipeline):
    """
    Customer data pipeline — F0101 → Odoo res.partner.

    Owns all customer-specific dependencies.
    SyncEngine receives this pipeline and calls the standard interface.
    """

    def __init__(
        self,
        source: str = "mock",
        dry_run: bool = False,
        limit: int | None = None,
    ):
        """
        Assemble the customer pipeline.

        Args:
            source (str):   'mock' reads from CSV, 'oracle' connects to JDE.
            dry_run (bool): If True, use CsvLoader instead of OdooLoader.
            limit (int):    Optional record limit for testing.
        """
        self._settings = get_settings()
        self._source   = source
        self._dry_run  = dry_run
        self._limit    = limit

        # ── Extractor ─────────────────────────────────────────────────
        if source == "mock":
            self._extractor = MockExtractor()
        else:
            from extractors.jde_extractor import JdeExtractor
            self._extractor = JdeExtractor(table="customers")

        # ── Transformer ───────────────────────────────────────────────
        self._transformer = CustomerTransformer()

        # ── Validator ─────────────────────────────────────────────────
        self._validator = CustomerValidator()

        # ── Loader ────────────────────────────────────────────────────
        if dry_run:
            self._loader = CsvLoader()
        else:
            self._loader = OdooLoader()

        logger.info(
            f"CustomerPipeline ready | "
            f"source: {source} | dry_run: {dry_run} | limit: {limit}"
        )

    @property
    def table_name(self) -> str:
        return "customers"

    @property
    def extractor(self):
        return self._extractor

    @property
    def transformer(self):
        return self._transformer

    @property
    def validator(self):
        return self._validator

    @property
    def loader(self):
        return self._loader

    def compute_watermark(
        self,
        records: list[dict],
        current: SyncWatermark,
    ) -> SyncWatermark:
        """
        Compute new watermark from UPMJ/UPMT fields in F0101 records.

        Pipeline owns this — SyncEngine must not know JDE field names.
        Falls back to current watermark if no valid timestamps found.

        Args:
            records: Raw extracted F0101 records
            current: Current watermark to fall back to

        Returns:
            SyncWatermark: New watermark from max UPMJ/UPMT in batch
        """
        max_upmj = current.last_upmj
        max_upmt = current.last_upmt

        for record in records:
            try:
                upmj = int(record.get("UPMJ") or 0)
                upmt = int(record.get("UPMT") or 0)
                if upmj > max_upmj or (upmj == max_upmj and upmt > max_upmt):
                    max_upmj = upmj
                    max_upmt = upmt
            except (ValueError, TypeError):
                continue

        return SyncWatermark(
            table_name=current.table_name,
            last_upmj=max_upmj,
            last_upmt=max_upmt,
            last_run_at=None,
            records_synced=0,
        )

    def describe(self) -> str:
        return (
            f"CustomerPipeline | "
            f"F0101 → res.partner | "
            f"source: {self._source} | "
            f"dry_run: {self._dry_run}"
        )
    