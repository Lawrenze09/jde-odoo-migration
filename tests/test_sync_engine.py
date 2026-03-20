"""
tests/test_sync_engine.py

Tests for SyncEngine — outcome classification and watermark computation.
Uses tmp_path for isolated SQLite databases so tests never affect the
real logs/transaction_log.db file.

OdooLoader and MockExtractor are not called in most tests — we test
the engine's orchestration logic and outcome mapping in isolation.
"""

import pytest
import csv
import os
from unittest.mock import patch, MagicMock
from sync.sync_engine import SyncEngine, SyncOutcome, SyncResult
from sync.sync_log import SyncLog
from loaders.odoo_loader import LoadResult, LoadStatus, RecordResult


@pytest.fixture
def temp_db(tmp_path):
    """Return a path to a temporary SQLite database."""
    return str(tmp_path / "test_sync.db")


@pytest.fixture
def temp_csv(tmp_path):
    """
    Create a minimal mock CSV with two records newer than watermark 126072/28800.
    AN8=1020: UPMJ=126073 (newer date)
    AN8=1021: UPMJ=126072, UPMT=36000 (same date, later time)
    """
    csv_path = str(tmp_path / "F0101.csv")
    rows = [
        {
            "AN8": "1020", "ALPH": "Robinsons Galleria", "AT1": "C",
            "PH1": "09301234567", "ADD1": "Ortigas Center", "ADD2": "",
            "CTY1": "Pasig City", "ADDS": "00", "ADDZ": "1605",
            "COUN": "PHL", "TAX": "300400500600", "PA8": "0",
            "UPMJ": "126073", "UPMT": "32400",
        },
        {
            "AN8": "1021", "ALPH": "SM City Cebu", "AT1": "C",
            "PH1": "09321234567", "ADD1": "North Reclamation Area", "ADD2": "",
            "CTY1": "Cebu City", "ADDS": "07", "ADDZ": "6000",
            "COUN": "PHL", "TAX": "400500600700", "PA8": "0",
            "UPMJ": "126072", "UPMT": "36000",
        },
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return csv_path


class TestSyncEngineNoOp:
    def test_no_op_when_watermark_is_current(self, temp_db, temp_csv):
        """
        When watermark equals the max UPMJ/UPMT in the CSV,
        the extractor returns 0 records and outcome is NO_OP.
        """
        # Set watermark ahead of all records in the CSV
        sync_log = SyncLog(db_path=temp_db)
        sync_log.update_watermark("F0101", last_upmj=126999, last_upmt=99999, records_synced=0)

        engine = SyncEngine(source="mock", dry_run=True, generate_report=False)
        engine.sync_log = sync_log

        with patch("sync.sync_engine.MockExtractor") as MockExt:
            mock_instance = MagicMock()
            mock_instance.extract.return_value = []
            MockExt.return_value = mock_instance

            result = engine.run()

        assert result.outcome == SyncOutcome.NO_OP
        assert result.records_extracted == 0
        assert result.message == "No new or updated records — sync is up to date"

    def test_no_op_exit_code_is_zero(self, temp_db):
        """NO_OP should map to exit code 0 — not a failure."""
        failed_outcomes = {SyncOutcome.FAILED, SyncOutcome.PARTIAL}
        assert SyncOutcome.NO_OP not in failed_outcomes


class TestSyncOutcomeClassification:
    def test_classify_failed_when_failed_gt_zero(self):
        """Any Odoo failure → FAILED outcome."""
        engine = SyncEngine(source="mock", dry_run=False, generate_report=False)
        load_result = LoadResult(batch_id="test", total=5, loaded=3, failed=1)
        assert engine._classify_outcome(load_result) == SyncOutcome.FAILED

    def test_classify_partial_when_not_processed_gt_zero(self):
        """Any NOT_PROCESSED records → PARTIAL outcome."""
        engine = SyncEngine(source="mock", dry_run=False, generate_report=False)
        load_result = LoadResult(batch_id="test", total=5, loaded=3, not_processed=2)
        assert engine._classify_outcome(load_result) == SyncOutcome.PARTIAL

    def test_classify_success_with_skips(self):
        """Loaded > 0 and skipped > 0 → SUCCESS_WITH_SKIPS."""
        engine = SyncEngine(source="mock", dry_run=False, generate_report=False)
        load_result = LoadResult(batch_id="test", total=5, loaded=3, skipped=2)
        assert engine._classify_outcome(load_result) == SyncOutcome.SUCCESS_WITH_SKIPS

    def test_classify_success_clean(self):
        """Loaded > 0, nothing else → SUCCESS."""
        engine = SyncEngine(source="mock", dry_run=False, generate_report=False)
        load_result = LoadResult(batch_id="test", total=3, loaded=3)
        assert engine._classify_outcome(load_result) == SyncOutcome.SUCCESS

    def test_classify_no_op_when_all_skipped(self):
        """Nothing loaded, all skipped → NO_OP."""
        engine = SyncEngine(source="mock", dry_run=False, generate_report=False)
        load_result = LoadResult(batch_id="test", total=3, skipped=3)
        assert engine._classify_outcome(load_result) == SyncOutcome.NO_OP


class TestComputeNewWatermark:
    def test_watermark_advances_to_max_upmj(self, temp_db):
        """New watermark should be the maximum UPMJ seen in the batch."""
        engine = SyncEngine(source="mock", dry_run=True, generate_report=False)
        engine.sync_log = SyncLog(db_path=temp_db)

        from sync.sync_log import SyncWatermark
        current = SyncWatermark("F0101", 126070, 28800, None, 0)

        records = [
            {"AN8": "1001", "UPMJ": "126072", "UPMT": "28800"},
            {"AN8": "1002", "UPMJ": "126073", "UPMT": "32400"},
            {"AN8": "1003", "UPMJ": "126071", "UPMT": "36000"},
        ]

        new_upmj, new_upmt = engine._compute_new_watermark(records, current)
        assert new_upmj == 126073
        assert new_upmt == 32400

    def test_watermark_uses_max_upmt_for_same_upmj(self, temp_db):
        """When multiple records share max UPMJ, use the max UPMT."""
        engine = SyncEngine(source="mock", dry_run=True, generate_report=False)
        engine.sync_log = SyncLog(db_path=temp_db)

        from sync.sync_log import SyncWatermark
        current = SyncWatermark("F0101", 126070, 0, None, 0)

        records = [
            {"AN8": "1001", "UPMJ": "126072", "UPMT": "28800"},
            {"AN8": "1002", "UPMJ": "126072", "UPMT": "36000"},
            {"AN8": "1003", "UPMJ": "126072", "UPMT": "14400"},
        ]

        new_upmj, new_upmt = engine._compute_new_watermark(records, current)
        assert new_upmj == 126072
        assert new_upmt == 36000

    def test_watermark_unchanged_when_records_have_no_upmj(self, temp_db):
        """Records with missing UPMJ should not change the watermark."""
        engine = SyncEngine(source="mock", dry_run=True, generate_report=False)
        engine.sync_log = SyncLog(db_path=temp_db)

        from sync.sync_log import SyncWatermark
        current = SyncWatermark("F0101", 126072, 28800, None, 0)

        records = [
            {"AN8": "1001", "UPMJ": "", "UPMT": ""},
            {"AN8": "1002", "UPMJ": None, "UPMT": None},
        ]

        new_upmj, new_upmt = engine._compute_new_watermark(records, current)
        assert new_upmj == 126072
        assert new_upmt == 28800


class TestDryRunOutcome:
    def test_dry_run_never_calls_odoo_loader(self, temp_db):
        """Dry run must not instantiate OdooLoader under any circumstances."""
        engine = SyncEngine(source="mock", dry_run=True, generate_report=False)
        engine.sync_log = SyncLog(db_path=temp_db)

        with patch("sync.sync_engine.MockExtractor") as MockExt, \
             patch("sync.sync_engine.OdooLoader") as MockLoader:

            mock_instance = MagicMock()
            mock_instance.extract.return_value = []
            MockExt.return_value = mock_instance

            engine.run()
            MockLoader.assert_not_called()

    def test_dry_run_outcome_is_no_op_when_nothing_extracted(self, temp_db):
        """Dry run with zero extracted records returns NO_OP not DRY_RUN."""
        engine = SyncEngine(source="mock", dry_run=True, generate_report=False)
        engine.sync_log = SyncLog(db_path=temp_db)

        with patch("sync.sync_engine.MockExtractor") as MockExt:
            mock_instance = MagicMock()
            mock_instance.extract.return_value = []
            MockExt.return_value = mock_instance

            result = engine.run()

        assert result.outcome == SyncOutcome.NO_OP
        