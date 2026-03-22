"""
pipelines/base_pipeline.py

Abstract base class for all migration pipelines.

A pipeline owns domain-specific execution:
    - Extraction with watermark filtering (knows JDE schema)
    - Transformation (knows JDE field formats)
    - Validation (knows business rules)
    - Loading (knows Odoo model structure)
    - Domain registries (UomRegistry, etc.)

SyncEngine owns domain-agnostic orchestration:
    - Watermark read/write
    - NO_OP detection
    - Outcome classification
    - Reporting trigger

SyncEngine calls pipeline.extractor.extract(watermark) —
it never knows what UPMJ, ITM, or AN8 mean.
"""

from abc import ABC, abstractmethod
from extractors.base_extractor import BaseExtractor
from utils.logger import get_logger
from sync.sync_log import SyncWatermark


logger = get_logger(__name__)


class BasePipeline(ABC):
    """
    Abstract pipeline — one concrete subclass per JDE table.

    Subclasses must provide extractor, transformer, validator, loader.
    SyncEngine interacts with pipelines only through this interface.
    """

    @property
    @abstractmethod
    def table_name(self) -> str:
        """
        Logical table name used as SyncLog watermark key.
        Must match the key used in SyncLog — 'customers' or 'items'.

        Returns:
            str: Table identifier e.g. 'customers', 'items'
        """

    @property
    @abstractmethod
    def extractor(self) -> BaseExtractor:
        """
        Extractor instance — called by SyncEngine as:
            records = pipeline.extractor.extract(last_upmj, last_upmt)

        The extractor owns incremental filtering logic.
        SyncEngine passes the watermark values but never interprets them.

        Returns:
            BaseExtractor: Configured extractor for this table.
        """

    @property
    @abstractmethod
    def transformer(self):
        """
        Transformer instance — called by SyncEngine as:
            transformed = pipeline.transformer.transform_batch(records)

        Returns:
            Transformer with transform_batch(records) -> list[dict]
        """

    @property
    @abstractmethod
    def validator(self):
        """
        Validator instance — called by SyncEngine as:
            valid, failed = pipeline.validator.validate_batch(transformed)

        Returns:
            Validator with validate_batch(records) -> tuple[list, list]
        """

    @property
    @abstractmethod
    def loader(self):
        """
        Loader instance — called by SyncEngine as:
            result = pipeline.loader.load(valid_records)

        Returns:
            Loader with load(records) -> LoadResult
        """

    def describe(self) -> str:
        """
        Human-readable description for logs and reports.
        Subclasses can override for custom descriptions.

        Returns:
            str: e.g. 'CustomerPipeline (F0101 → res.partner)'
        """
        return f"{self.__class__.__name__} (table: {self.table_name})"
    

    @abstractmethod
    def compute_watermark(
        self,
        records: list[dict],
        current: SyncWatermark,
    ) -> SyncWatermark:
        """
        Compute new watermark from extracted records.
        Pipeline owns this — SyncEngine must not know JDE field names.

        Args:
            records: Extracted records (raw JDE format)
            current: Current watermark to fall back to

        Returns:
            SyncWatermark: New watermark from max timestamp in batch
        """
        