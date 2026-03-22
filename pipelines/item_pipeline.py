"""
pipelines/item_pipeline.py

Item migration pipeline — F4101 → Odoo product.template.

Assembles the full item stack:
    MockExtractor / JdeExtractor → ItemTransformer →
    ItemValidator → ItemLoader / CsvLoader

Owns UomRegistry — built once at pipeline init and shared
between ItemValidator and ItemLoader. This is the key difference
from CustomerPipeline — items have a domain registry dependency.

UomRegistry initialization:
    Live mode  — connects to Odoo for accurate ID resolution
    Dry run    — builds from mapping CSV with mock UOM records
                 so dry runs work even when Odoo is unreachable

SyncEngine knows nothing about UomRegistry or JDE schema.
"""

import csv as csv_module
import xmlrpc.client
from unittest.mock import MagicMock
from pipelines.base_pipeline import BasePipeline
from extractors.mock_extractor import MockExtractor
from transformers.item_transformer import ItemTransformer
from validators.item_validator import ItemValidator
from loaders.item_loader import ItemLoader
from loaders.uom_registry import UomRegistry
from loaders.csv_loader import CsvLoader
from sync.sync_log import SyncWatermark
from config.settings import get_settings
from utils.logger import get_logger

logger = get_logger(__name__)


class ItemPipeline(BasePipeline):
    """
    Item data pipeline — F4101 → Odoo product.template.

    Owns UomRegistry — built at init, shared by validator and loader.
    SyncEngine calls the standard interface — never touches UomRegistry.
    """

    def __init__(
        self,
        source: str = "mock",
        dry_run: bool = False,
        limit: int | None = None,
    ):
        """
        Assemble the item pipeline.

        In live mode, connects to Odoo to build UomRegistry with real
        integer IDs for payload construction.

        In dry run mode, builds UomRegistry from the mapping CSV using
        mock Odoo UOM records — no Odoo connection required. UOM IDs
        will be sequential integers (not real Odoo IDs) but category
        validation still works correctly since categories come from CSV.

        Args:
            source (str):   'mock' reads from CSV, 'oracle' connects to JDE.
            dry_run (bool): If True, use CsvLoader instead of ItemLoader.
            limit (int):    Optional record limit for testing.
        """
        self._settings = get_settings()
        self._source   = source
        self._dry_run  = dry_run
        self._limit    = limit

        # ── UomRegistry ───────────────────────────────────────────────
        # Live mode: connect to Odoo for accurate ID resolution.
        # Dry run:   build from mapping CSV — no Odoo connection needed.
        #            This allows dry runs to work even when Odoo is down.
        if not dry_run:
            url    = self._settings.odoo_url
            db     = self._settings.odoo_db
            common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
            uid    = common.authenticate(
                db,
                self._settings.odoo_username,
                self._settings.odoo_password,
                {}
            )
            models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")
            self._uom_registry = UomRegistry(
                models, uid, self._settings.odoo_password, db
            )
            logger.info("UomRegistry built from live Odoo")
        else:
            # Dry run — build registry from mapping CSV.
            # Read unique Odoo UOM names from the mapping file and
            # create mock Odoo records with sequential IDs.
            # IDs are not written to Odoo in dry run mode — only
            # category validation matters here.
            mapping_path = "config/uom_mapping.csv"
            uom_names    = []
            with open(mapping_path, newline="", encoding="utf-8") as f:
                reader = csv_module.DictReader(f)
                for row in reader:
                    uom_names.append(row["odoo_name"].strip())

            unique_names = list(dict.fromkeys(uom_names))  # preserve order, dedup
            mock_models  = MagicMock()
            mock_models.execute_kw.return_value = [
                {"id": i + 1, "name": name}
                for i, name in enumerate(unique_names)
            ]
            self._uom_registry = UomRegistry(
                mock_models,
                uid=0,
                password="",
                db="",
                mapping_path=mapping_path,
            )
            logger.info("UomRegistry built from mapping CSV (dry run mode)")

        # ── Extractor ─────────────────────────────────────────────────
        if source == "mock":
            self._extractor = MockExtractor(
                file_path=self._settings.mock_data_path.replace("F0101", "F4101")
            )
        else:
            from extractors.jde_extractor import JdeExtractor
            self._extractor = JdeExtractor(table="items")

        # ── Transformer ───────────────────────────────────────────────
        self._transformer = ItemTransformer()

        # ── Validator ─────────────────────────────────────────────────
        self._validator = ItemValidator(uom_registry=self._uom_registry)

        # ── Loader ────────────────────────────────────────────────────
        if dry_run:
            self._loader = CsvLoader()
        else:
            self._loader = ItemLoader(uom_registry=self._uom_registry)

        logger.info(
            f"ItemPipeline ready | "
            f"source: {source} | dry_run: {dry_run} | limit: {limit}"
        )

    @property
    def table_name(self) -> str:
        return "items"

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
        Compute new watermark from UPMJ/UPMT fields in F4101 records.

        Pipeline owns this — SyncEngine must not know JDE field names.
        Falls back to current watermark if no valid timestamps found.

        Args:
            records: Raw extracted F4101 records
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
            f"ItemPipeline | "
            f"F4101 → product.template | "
            f"source: {self._source} | "
            f"dry_run: {self._dry_run}"
        )
    