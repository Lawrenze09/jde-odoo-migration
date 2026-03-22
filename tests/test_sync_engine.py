"""
tests/test_sync_engine.py

Tests for SyncEngine — pipeline-driven orchestration, NO_OP detection,
outcome classification, and watermark advancement.

Uses mock pipelines so tests never require a live Odoo connection,
real extractors, or any domain-specific logic. SyncEngine is tested
in complete isolation from all pipeline implementations.
"""

import pytest
from unittest.mock import MagicMock
from sync.sync_engine import SyncEngine, SyncOutcome, SyncResult
from sync.sync_log import SyncWatermark
from loaders.odoo_loader import LoadResult


def make_load_result(
    loaded=0, failed=0, not_processed=0, skipped=0
) -> LoadResult:
    """Build a LoadResult with specified counts."""
    r = LoadResult(
        batch_id="test-batch",
        total=loaded + failed + not_processed + skipped
    )
    r.loaded        = loaded
    r.failed        = failed
    r.not_processed = not_processed
    r.skipped       = skipped
    return r


def make_watermark(table_name="customers", upmj=0, upmt=0) -> SyncWatermark:
    """Build a SyncWatermark with optional fields defaulted."""
    return SyncWatermark(
        table_name=table_name,
        last_upmj=upmj,
        last_upmt=upmt,
        last_run_at=None,
        records_synced=0,
    )


def make_mock_pipeline(
    table_name: str = "customers",
    records: list | None = None,
    load_result: LoadResult | None = None,
) -> MagicMock:
    """
    Build a mock pipeline that implements the BasePipeline interface.
    SyncEngine never calls concrete pipeline methods directly —
    it always goes through extractor, transformer, validator, loader.
    """
    records     = records or []
    load_result = load_result or make_load_result(loaded=len(records))

    pipeline = MagicMock()
    pipeline.table_name = table_name
    pipeline.describe.return_value = f"MockPipeline ({table_name})"

    # Extractor returns the provided records
    pipeline.extractor.extract.return_value = records

    # Transformer returns records unchanged
    pipeline.transformer.transform_batch.side_effect = lambda r: r

    # Validator passes all records as valid
    pipeline.validator.validate_batch.side_effect = lambda r: (r, [])

    # Loader returns the provided load result
    pipeline.loader.load.return_value = load_result

    # compute_watermark returns a zero watermark by default
    pipeline.compute_watermark.return_value = make_watermark(table_name)

    return pipeline


# ── Sample records for watermark tests ───────────────────────────────────────
SAMPLE_RECORDS = [
    {"AN8": "1001", "UPMJ": "126070", "UPMT": "28800"},
    {"AN8": "1002", "UPMJ": "126072", "UPMT": "36000"},
    {"AN8": "1003", "UPMJ": "126073", "UPMT": "14400"},
]


class TestSyncEngineNoOp:
    def test_no_op_when_no_records_extracted(self, tmp_path):
        """Zero extracted records must return NO_OP immediately."""
        pipeline = make_mock_pipeline(records=[])
        engine   = SyncEngine(
            pipeline=pipeline,
            sync_log_path=str(tmp_path / "sync.db")
        )
        result = engine.run()
        assert result.outcome == SyncOutcome.NO_OP

    def test_no_op_exit_code_is_zero(self, tmp_path):
        """NO_OP must return exit code 0 — not a failure."""
        pipeline = make_mock_pipeline(records=[])
        engine   = SyncEngine(
            pipeline=pipeline,
            sync_log_path=str(tmp_path / "sync.db")
        )
        result = engine.run()
        assert result.exit_code == 0

    def test_no_op_does_not_call_transformer(self, tmp_path):
        """Transformer must not be called when zero records extracted."""
        pipeline = make_mock_pipeline(records=[])
        engine   = SyncEngine(
            pipeline=pipeline,
            sync_log_path=str(tmp_path / "sync.db")
        )
        engine.run()
        pipeline.transformer.transform_batch.assert_not_called()

    def test_no_op_does_not_call_loader(self, tmp_path):
        """Loader must not be called when zero records extracted."""
        pipeline = make_mock_pipeline(records=[])
        engine   = SyncEngine(
            pipeline=pipeline,
            sync_log_path=str(tmp_path / "sync.db")
        )
        engine.run()
        pipeline.loader.load.assert_not_called()

    def test_no_op_watermark_not_advanced(self, tmp_path):
        """Watermark must not be advanced on NO_OP."""
        pipeline = make_mock_pipeline(records=[])
        engine   = SyncEngine(
            pipeline=pipeline,
            sync_log_path=str(tmp_path / "sync.db")
        )
        result = engine.run()
        assert result.watermark_after is None


class TestSyncOutcomeClassification:
    def test_classify_failed_when_failed_gt_zero_no_not_processed(self, tmp_path):
        """failed > 0 and not_processed == 0 → FAILED."""
        load_result = make_load_result(loaded=3, failed=1, not_processed=0)
        pipeline    = make_mock_pipeline(
            records=SAMPLE_RECORDS,
            load_result=load_result,
        )
        engine = SyncEngine(
            pipeline=pipeline,
            sync_log_path=str(tmp_path / "sync.db")
        )
        result = engine.run()
        assert result.outcome == SyncOutcome.FAILED

    def test_classify_partial_when_failed_and_not_processed(self, tmp_path):
        """failed > 0 AND not_processed > 0 → PARTIAL (batch stopped mid-run)."""
        load_result = make_load_result(loaded=2, failed=1, not_processed=3)
        pipeline    = make_mock_pipeline(
            records=SAMPLE_RECORDS,
            load_result=load_result,
        )
        engine = SyncEngine(
            pipeline=pipeline,
            sync_log_path=str(tmp_path / "sync.db")
        )
        result = engine.run()
        assert result.outcome == SyncOutcome.PARTIAL

    def test_classify_success_with_skips(self, tmp_path):
        """loaded > 0 and skipped > 0 → SUCCESS_WITH_SKIPS."""
        load_result = make_load_result(loaded=2, skipped=1)
        pipeline    = make_mock_pipeline(
            records=SAMPLE_RECORDS,
            load_result=load_result,
        )
        engine = SyncEngine(
            pipeline=pipeline,
            sync_log_path=str(tmp_path / "sync.db")
        )
        result = engine.run()
        assert result.outcome == SyncOutcome.SUCCESS_WITH_SKIPS

    def test_classify_success_with_skips_when_all_skipped(self, tmp_path):
        """All skipped and none loaded → SUCCESS_WITH_SKIPS, not NO_OP."""
        load_result = make_load_result(loaded=0, skipped=3)
        pipeline    = make_mock_pipeline(
            records=SAMPLE_RECORDS,
            load_result=load_result,
        )
        engine = SyncEngine(
            pipeline=pipeline,
            sync_log_path=str(tmp_path / "sync.db")
        )
        result = engine.run()
        assert result.outcome == SyncOutcome.SUCCESS_WITH_SKIPS

    def test_classify_success_clean(self, tmp_path):
        """All loaded, no failures, no skips → SUCCESS."""
        load_result = make_load_result(loaded=3)
        pipeline    = make_mock_pipeline(
            records=SAMPLE_RECORDS,
            load_result=load_result,
        )
        engine = SyncEngine(
            pipeline=pipeline,
            sync_log_path=str(tmp_path / "sync.db")
        )
        result = engine.run()
        assert result.outcome == SyncOutcome.SUCCESS

    def test_failed_exit_code_is_one(self, tmp_path):
        """FAILED outcome must return exit code 1."""
        load_result = make_load_result(loaded=0, failed=3)
        pipeline    = make_mock_pipeline(
            records=SAMPLE_RECORDS,
            load_result=load_result,
        )
        engine = SyncEngine(
            pipeline=pipeline,
            sync_log_path=str(tmp_path / "sync.db")
        )
        result = engine.run()
        assert result.exit_code == 1

    def test_partial_exit_code_is_one(self, tmp_path):
        """PARTIAL outcome must return exit code 1."""
        load_result = make_load_result(loaded=1, failed=1, not_processed=2)
        pipeline    = make_mock_pipeline(
            records=SAMPLE_RECORDS,
            load_result=load_result,
        )
        engine = SyncEngine(
            pipeline=pipeline,
            sync_log_path=str(tmp_path / "sync.db")
        )
        result = engine.run()
        assert result.exit_code == 1

    def test_success_exit_code_is_zero(self, tmp_path):
        """SUCCESS outcome must return exit code 0."""
        load_result = make_load_result(loaded=3)
        pipeline    = make_mock_pipeline(
            records=SAMPLE_RECORDS,
            load_result=load_result,
        )
        engine = SyncEngine(
            pipeline=pipeline,
            sync_log_path=str(tmp_path / "sync.db")
        )
        result = engine.run()
        assert result.exit_code == 0


class TestWatermarkAdvancement:
    def test_watermark_advances_on_success(self, tmp_path):
        """Watermark must advance after successful load."""
        load_result = make_load_result(loaded=3)
        pipeline    = make_mock_pipeline(
            records=SAMPLE_RECORDS,
            load_result=load_result,
        )
        pipeline.compute_watermark.return_value = make_watermark(
            table_name="customers",
            upmj=126073,
            upmt=14400,
        )
        engine = SyncEngine(
            pipeline=pipeline,
            sync_log_path=str(tmp_path / "sync.db")
        )
        result = engine.run()
        assert result.watermark_after is not None
        assert result.watermark_after.last_upmj == 126073

    def test_watermark_not_advanced_on_failure(self, tmp_path):
        """Watermark must NOT advance when any record fails."""
        load_result = make_load_result(loaded=2, failed=1)
        pipeline    = make_mock_pipeline(
            records=SAMPLE_RECORDS,
            load_result=load_result,
        )
        engine = SyncEngine(
            pipeline=pipeline,
            sync_log_path=str(tmp_path / "sync.db")
        )
        result = engine.run()
        assert result.watermark_after is None

    def test_compute_watermark_delegated_to_pipeline(self, tmp_path):
        """SyncEngine must delegate watermark computation to the pipeline."""
        load_result = make_load_result(loaded=3)
        pipeline    = make_mock_pipeline(
            records=SAMPLE_RECORDS,
            load_result=load_result,
        )
        engine = SyncEngine(
            pipeline=pipeline,
            sync_log_path=str(tmp_path / "sync.db")
        )
        engine.run()
        pipeline.compute_watermark.assert_called_once()


class TestDryRunOutcome:
    def test_dry_run_returns_dry_run_outcome(self, tmp_path):
        """Dry run must return DRY_RUN outcome regardless of records."""
        pipeline = make_mock_pipeline(records=SAMPLE_RECORDS)
        engine   = SyncEngine(
            pipeline=pipeline,
            dry_run=True,
            sync_log_path=str(tmp_path / "sync.db")
        )
        result = engine.run()
        assert result.outcome == SyncOutcome.DRY_RUN

    def test_dry_run_never_calls_loader(self, tmp_path):
        """Loader must never be called in dry run mode."""
        pipeline = make_mock_pipeline(records=SAMPLE_RECORDS)
        engine   = SyncEngine(
            pipeline=pipeline,
            dry_run=True,
            sync_log_path=str(tmp_path / "sync.db")
        )
        engine.run()
        pipeline.loader.load.assert_not_called()

    def test_dry_run_exit_code_is_zero(self, tmp_path):
        """Dry run must always return exit code 0."""
        pipeline = make_mock_pipeline(records=SAMPLE_RECORDS)
        engine   = SyncEngine(
            pipeline=pipeline,
            dry_run=True,
            sync_log_path=str(tmp_path / "sync.db")
        )
        result = engine.run()
        assert result.exit_code == 0

    def test_dry_run_watermark_not_advanced(self, tmp_path):
        """Watermark must not advance in dry run mode."""
        pipeline = make_mock_pipeline(records=SAMPLE_RECORDS)
        engine   = SyncEngine(
            pipeline=pipeline,
            dry_run=True,
            sync_log_path=str(tmp_path / "sync.db")
        )
        result = engine.run()
        assert result.watermark_after is None
        