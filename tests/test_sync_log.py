"""
tests/test_sync_log.py

Tests for SyncLog watermark read/write behavior.
Uses a temporary in-memory SQLite path so tests never touch
the real logs/transaction_log.db file.
"""

import pytest
import os
import tempfile
from sync.sync_log import SyncLog, SyncWatermark


@pytest.fixture
def sync_log(tmp_path):
    """
    Return a SyncLog instance backed by a temporary SQLite file.
    tmp_path is a pytest built-in fixture that provides a unique
    temporary directory per test — no cleanup needed.
    """
    db_path = str(tmp_path / "test_sync.db")
    return SyncLog(db_path=db_path)


class TestGetWatermark:
    def test_returns_zero_watermark_on_first_run(self, sync_log):
        """First run — no watermark exists, should return zeros."""
        watermark = sync_log.get_watermark("F0101")
        assert watermark.last_upmj == 0
        assert watermark.last_upmt == 0
        assert watermark.last_run_at is None
        assert watermark.records_synced == 0

    def test_returns_correct_table_name(self, sync_log):
        """Watermark table_name must match the requested table."""
        watermark = sync_log.get_watermark("F0101")
        assert watermark.table_name == "F0101"

    def test_different_tables_return_independent_watermarks(self, sync_log):
        """F0101 and F4101 watermarks must be independent."""
        sync_log.update_watermark("F0101", 126072, 28800, 13)
        f0101 = sync_log.get_watermark("F0101")
        f4101 = sync_log.get_watermark("F4101")
        assert f0101.last_upmj == 126072
        assert f4101.last_upmj == 0


class TestUpdateWatermark:
    def test_watermark_saves_correctly(self, sync_log):
        """After update, get_watermark must return the saved values."""
        sync_log.update_watermark("F0101", 126072, 28800, 13)
        watermark = sync_log.get_watermark("F0101")
        assert watermark.last_upmj == 126072
        assert watermark.last_upmt == 28800
        assert watermark.records_synced == 13

    def test_second_update_overwrites_first(self, sync_log):
        """Each run overwrites the previous watermark — no duplicates."""
        sync_log.update_watermark("F0101", 126072, 28800, 13)
        sync_log.update_watermark("F0101", 126073, 36000, 3)
        watermark = sync_log.get_watermark("F0101")
        assert watermark.last_upmj == 126073
        assert watermark.last_upmt == 36000
        assert watermark.records_synced == 3

    def test_last_run_at_is_set_after_update(self, sync_log):
        """last_run_at must be populated after an update."""
        sync_log.update_watermark("F0101", 126072, 28800, 13)
        watermark = sync_log.get_watermark("F0101")
        assert watermark.last_run_at is not None

    def test_zero_records_synced_is_valid(self, sync_log):
        """A run that processes zero records must still update watermark."""
        sync_log.update_watermark("F0101", 126072, 28800, 0)
        watermark = sync_log.get_watermark("F0101")
        assert watermark.records_synced == 0


class TestGetAllWatermarks:
    def test_returns_empty_list_when_no_syncs(self, sync_log):
        """No syncs yet — should return empty list."""
        watermarks = sync_log.get_all_watermarks()
        assert watermarks == []

    def test_returns_all_synced_tables(self, sync_log):
        """After syncing two tables, both should appear."""
        sync_log.update_watermark("F0101", 126072, 28800, 13)
        sync_log.update_watermark("F4101", 126010, 36000, 45)
        watermarks = sync_log.get_all_watermarks()
        assert len(watermarks) == 2
        table_names = [w.table_name for w in watermarks]
        assert "F0101" in table_names
        assert "F4101" in table_names
        