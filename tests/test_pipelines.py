"""
tests/test_pipelines.py

Tests for CustomerPipeline and ItemPipeline — assembly, interface
compliance, and watermark computation.

These tests verify that pipelines correctly implement BasePipeline,
assemble their components, and compute watermarks accurately.
No live Odoo connection required — uses mocks for all external calls.
"""

import pytest
from unittest.mock import MagicMock, patch
from sync.sync_log import SyncWatermark


def make_watermark(
    table_name: str = "customers",
    upmj: int = 0,
    upmt: int = 0,
) -> SyncWatermark:
    """Build a SyncWatermark with all required fields defaulted."""
    return SyncWatermark(
        table_name=table_name,
        last_upmj=upmj,
        last_upmt=upmt,
        last_run_at=None,
        records_synced=0,
    )


# ── Shared fixtures ──────────────────────────────────────────────────────────

ZERO_WATERMARK = make_watermark("customers", 0, 0)

SAMPLE_RECORDS = [
    {"AN8": "1001", "UPMJ": "126070", "UPMT": "28800"},
    {"AN8": "1002", "UPMJ": "126072", "UPMT": "36000"},
    {"AN8": "1003", "UPMJ": "126073", "UPMT": "14400"},
]


# ── CustomerPipeline tests ───────────────────────────────────────────────────

class TestCustomerPipelineAssembly:
    def test_table_name_is_customers(self):
        from pipelines.customer_pipeline import CustomerPipeline
        pipeline = CustomerPipeline(source="mock", dry_run=True)
        assert pipeline.table_name == "customers"

    def test_extractor_is_present(self):
        from pipelines.customer_pipeline import CustomerPipeline
        pipeline = CustomerPipeline(source="mock", dry_run=True)
        assert pipeline.extractor is not None

    def test_transformer_is_present(self):
        from pipelines.customer_pipeline import CustomerPipeline
        pipeline = CustomerPipeline(source="mock", dry_run=True)
        assert pipeline.transformer is not None

    def test_validator_is_present(self):
        from pipelines.customer_pipeline import CustomerPipeline
        pipeline = CustomerPipeline(source="mock", dry_run=True)
        assert pipeline.validator is not None

    def test_loader_is_present(self):
        from pipelines.customer_pipeline import CustomerPipeline
        pipeline = CustomerPipeline(source="mock", dry_run=True)
        assert pipeline.loader is not None

    def test_dry_run_uses_csv_loader(self):
        from pipelines.customer_pipeline import CustomerPipeline
        from loaders.csv_loader import CsvLoader
        pipeline = CustomerPipeline(source="mock", dry_run=True)
        assert isinstance(pipeline.loader, CsvLoader)

    def test_live_run_uses_odoo_loader(self):
        from pipelines.customer_pipeline import CustomerPipeline
        from loaders.odoo_loader import OdooLoader
        pipeline = CustomerPipeline(source="mock", dry_run=False)
        assert isinstance(pipeline.loader, OdooLoader)

    def test_describe_contains_table_name(self):
        from pipelines.customer_pipeline import CustomerPipeline
        pipeline = CustomerPipeline(source="mock", dry_run=True)
        assert "F0101" in pipeline.describe()
        assert "res.partner" in pipeline.describe()


class TestCustomerPipelineWatermark:
    def test_watermark_advances_to_max_upmj(self):
        from pipelines.customer_pipeline import CustomerPipeline
        pipeline  = CustomerPipeline(source="mock", dry_run=True)
        watermark = pipeline.compute_watermark(SAMPLE_RECORDS, ZERO_WATERMARK)
        assert watermark.last_upmj == 126073

    def test_watermark_uses_max_upmt_for_same_upmj(self):
        from pipelines.customer_pipeline import CustomerPipeline
        records = [
            {"UPMJ": "126072", "UPMT": "28800"},
            {"UPMJ": "126072", "UPMT": "36000"},
        ]
        pipeline  = CustomerPipeline(source="mock", dry_run=True)
        current   = make_watermark("customers", 126072, 28800)
        watermark = pipeline.compute_watermark(records, current)
        assert watermark.last_upmt == 36000

    def test_watermark_unchanged_when_records_have_no_upmj(self):
        from pipelines.customer_pipeline import CustomerPipeline
        records  = [{"AN8": "1001"}]
        pipeline = CustomerPipeline(source="mock", dry_run=True)
        current  = make_watermark("customers", 126072, 28800)
        result   = pipeline.compute_watermark(records, current)
        assert result.last_upmj == 126072
        assert result.last_upmt == 28800

    def test_watermark_table_name_preserved(self):
        from pipelines.customer_pipeline import CustomerPipeline
        pipeline  = CustomerPipeline(source="mock", dry_run=True)
        watermark = pipeline.compute_watermark(
            SAMPLE_RECORDS,
            make_watermark("customers", 0, 0)
        )
        assert watermark.table_name == "customers"

    def test_empty_records_returns_current_watermark(self):
        from pipelines.customer_pipeline import CustomerPipeline
        pipeline = CustomerPipeline(source="mock", dry_run=True)
        current  = make_watermark("customers", 126070, 28800)
        result   = pipeline.compute_watermark([], current)
        assert result.last_upmj == 126070
        assert result.last_upmt == 28800


# ── ItemPipeline tests ───────────────────────────────────────────────────────

class TestItemPipelineAssembly:
    def test_table_name_is_items(self):
        from pipelines.item_pipeline import ItemPipeline
        pipeline = ItemPipeline(source="mock", dry_run=True)
        assert pipeline.table_name == "items"

    def test_extractor_is_present(self):
        from pipelines.item_pipeline import ItemPipeline
        pipeline = ItemPipeline(source="mock", dry_run=True)
        assert pipeline.extractor is not None

    def test_transformer_is_present(self):
        from pipelines.item_pipeline import ItemPipeline
        pipeline = ItemPipeline(source="mock", dry_run=True)
        assert pipeline.transformer is not None

    def test_validator_is_present(self):
        from pipelines.item_pipeline import ItemPipeline
        pipeline = ItemPipeline(source="mock", dry_run=True)
        assert pipeline.validator is not None

    def test_loader_is_present(self):
        from pipelines.item_pipeline import ItemPipeline
        pipeline = ItemPipeline(source="mock", dry_run=True)
        assert pipeline.loader is not None

    def test_dry_run_uses_csv_loader(self):
        from pipelines.item_pipeline import ItemPipeline
        from loaders.csv_loader import CsvLoader
        pipeline = ItemPipeline(source="mock", dry_run=True)
        assert isinstance(pipeline.loader, CsvLoader)

    def test_dry_run_does_not_connect_to_odoo(self):
        """
        ItemPipeline dry_run must not make any Odoo XML-RPC calls.
        Verifies operational resilience — dry run works without Odoo.
        """
        from pipelines.item_pipeline import ItemPipeline
        with patch("xmlrpc.client.ServerProxy") as mock_proxy:
            pipeline = ItemPipeline(source="mock", dry_run=True)
            mock_proxy.assert_not_called()

    def test_uom_registry_built_in_dry_run(self):
        """UomRegistry must be initialized even in dry run mode."""
        from pipelines.item_pipeline import ItemPipeline
        from loaders.uom_registry import UomRegistry
        pipeline = ItemPipeline(source="mock", dry_run=True)
        assert pipeline._uom_registry is not None
        assert isinstance(pipeline._uom_registry, UomRegistry)

    def test_describe_contains_table_name(self):
        from pipelines.item_pipeline import ItemPipeline
        pipeline = ItemPipeline(source="mock", dry_run=True)
        assert "F4101" in pipeline.describe()
        assert "product.template" in pipeline.describe()


class TestItemPipelineWatermark:
    def test_watermark_advances_to_max_upmj(self):
        from pipelines.item_pipeline import ItemPipeline
        records = [
            {"ITM": "2002", "UPMJ": "126070", "UPMT": "28800"},
            {"ITM": "2003", "UPMJ": "126072", "UPMT": "36000"},
            {"ITM": "2004", "UPMJ": "126073", "UPMT": "14400"},
        ]
        pipeline  = ItemPipeline(source="mock", dry_run=True)
        watermark = pipeline.compute_watermark(
            records,
            make_watermark("items", 0, 0)
        )
        assert watermark.last_upmj == 126073

    def test_watermark_table_name_is_items(self):
        from pipelines.item_pipeline import ItemPipeline
        pipeline  = ItemPipeline(source="mock", dry_run=True)
        watermark = pipeline.compute_watermark(
            [{"UPMJ": "126070", "UPMT": "28800"}],
            make_watermark("items", 0, 0)
        )
        assert watermark.table_name == "items"

    def test_empty_records_returns_current_watermark(self):
        from pipelines.item_pipeline import ItemPipeline
        pipeline = ItemPipeline(source="mock", dry_run=True)
        current  = make_watermark("items", 126070, 28800)
        result   = pipeline.compute_watermark([], current)
        assert result.last_upmj == 126070
        assert result.last_upmt == 28800


# ── BasePipeline contract compliance ─────────────────────────────────────────

class TestBasePipelineContract:
    def test_customer_pipeline_implements_base_pipeline(self):
        from pipelines.customer_pipeline import CustomerPipeline
        from pipelines.base_pipeline import BasePipeline
        pipeline = CustomerPipeline(source="mock", dry_run=True)
        assert isinstance(pipeline, BasePipeline)

    def test_item_pipeline_implements_base_pipeline(self):
        from pipelines.item_pipeline import ItemPipeline
        from pipelines.base_pipeline import BasePipeline
        pipeline = ItemPipeline(source="mock", dry_run=True)
        assert isinstance(pipeline, BasePipeline)

    def test_customer_pipeline_has_compute_watermark(self):
        from pipelines.customer_pipeline import CustomerPipeline
        pipeline = CustomerPipeline(source="mock", dry_run=True)
        assert callable(getattr(pipeline, "compute_watermark", None))

    def test_item_pipeline_has_compute_watermark(self):
        from pipelines.item_pipeline import ItemPipeline
        pipeline = ItemPipeline(source="mock", dry_run=True)
        assert callable(getattr(pipeline, "compute_watermark", None))
        