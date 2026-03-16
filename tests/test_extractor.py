"""
tests/test_extractor.py

Tests for MockExtractor — full load and incremental sync modes.
Uses a temporary CSV file so tests never depend on mock_data/F0101.csv.
Each test controls its own input data precisely.
"""

import pytest
import os
import csv
import tempfile
from extractors.mock_extractor import MockExtractor


@pytest.fixture
def sample_csv(tmp_path):
    """
    Create a minimal CSV file with known UPMJ/UPMT values for testing.
    Returns the file path.

    Records:
        AN8=1001 | UPMJ=126070 | UPMT=28800  — old record, before watermark
        AN8=1002 | UPMJ=126072 | UPMT=28800  — exact watermark match
        AN8=1003 | UPMJ=126072 | UPMT=36000  — same date, later time (passes)
        AN8=1004 | UPMJ=126073 | UPMT=14400  — newer date (passes)
    """
    csv_path = str(tmp_path / "test_F0101.csv")
    rows = [
        {
            "AN8": "1001", "ALPH": "Old Record", "AT1": "C",
            "PH1": "09171234567", "ADD1": "Street A", "ADD2": "",
            "CTY1": "Manila", "ADDS": "00", "ADDZ": "1000",
            "COUN": "PHL", "TAX": "111111111000", "PA8": "0",
            "UPMJ": "126070", "UPMT": "28800",
        },
        {
            "AN8": "1002", "ALPH": "Exact Watermark", "AT1": "C",
            "PH1": "09181234567", "ADD1": "Street B", "ADD2": "",
            "CTY1": "Cebu City", "ADDS": "07", "ADDZ": "6000",
            "COUN": "PHL", "TAX": "222222222000", "PA8": "0",
            "UPMJ": "126072", "UPMT": "28800",
        },
        {
            "AN8": "1003", "ALPH": "Same Date Later Time", "AT1": "C",
            "PH1": "09191234567", "ADD1": "Street C", "ADD2": "",
            "CTY1": "Davao City", "ADDS": "11", "ADDZ": "8000",
            "COUN": "PHL", "TAX": "333333333000", "PA8": "0",
            "UPMJ": "126072", "UPMT": "36000",
        },
        {
            "AN8": "1004", "ALPH": "Newer Date", "AT1": "C",
            "PH1": "09201234567", "ADD1": "Street D", "ADD2": "",
            "CTY1": "Quezon City", "ADDS": "00", "ADDZ": "1100",
            "COUN": "PHL", "TAX": "444444444000", "PA8": "0",
            "UPMJ": "126073", "UPMT": "14400",
        },
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return csv_path


@pytest.fixture
def extractor(sample_csv):
    """Return a MockExtractor pointed at the test CSV."""
    return MockExtractor(file_path=sample_csv)


class TestFullLoad:
    def test_returns_all_records_when_no_watermark(self, extractor):
        """Full load — no watermark — must return all 4 records."""
        records = extractor.extract()
        assert len(records) == 4

    def test_returns_all_records_when_watermark_is_zero(self, extractor):
        """Explicit zero watermark is the same as no watermark."""
        records = extractor.extract(last_upmj=0, last_upmt=0)
        assert len(records) == 4

    def test_records_have_jde_column_names(self, extractor):
        """Every record must have JDE column name keys."""
        records = extractor.extract()
        assert "AN8" in records[0]
        assert "ALPH" in records[0]
        assert "UPMJ" in records[0]
        assert "UPMT" in records[0]


class TestIncrementalFilter:
    def test_exact_watermark_match_excluded(self, extractor):
        """Record with UPMJ=126072 UPMT=28800 must be excluded — already processed."""
        records = extractor.extract(last_upmj=126072, last_upmt=28800)
        an8s = [r["AN8"] for r in records]
        assert "1002" not in an8s

    def test_older_record_excluded(self, extractor):
        """Record with UPMJ=126070 must be excluded — before watermark."""
        records = extractor.extract(last_upmj=126072, last_upmt=28800)
        an8s = [r["AN8"] for r in records]
        assert "1001" not in an8s

    def test_same_date_later_time_included(self, extractor):
        """Record with UPMJ=126072 UPMT=36000 must be included — same date, later time."""
        records = extractor.extract(last_upmj=126072, last_upmt=28800)
        an8s = [r["AN8"] for r in records]
        assert "1003" in an8s

    def test_newer_date_included(self, extractor):
        """Record with UPMJ=126073 must be included — newer date."""
        records = extractor.extract(last_upmj=126072, last_upmt=28800)
        an8s = [r["AN8"] for r in records]
        assert "1004" in an8s

    def test_incremental_returns_correct_count(self, extractor):
        """Watermark 126072/28800 must return exactly 2 records."""
        records = extractor.extract(last_upmj=126072, last_upmt=28800)
        assert len(records) == 2

    def test_future_watermark_returns_zero(self, extractor):
        """Watermark beyond all records must return zero records."""
        records = extractor.extract(last_upmj=999999, last_upmt=99999)
        assert len(records) == 0

    def test_none_values_in_upmj_included_for_safety(self, extractor, tmp_path):
        """Record with missing UPMJ must be included — fail safe over fail silent."""
        csv_path = str(tmp_path / "missing_upmj.csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "AN8", "ALPH", "AT1", "PH1", "ADD1", "ADD2",
                "CTY1", "ADDS", "ADDZ", "COUN", "TAX", "PA8",
                "UPMJ", "UPMT"
            ])
            writer.writeheader()
            writer.writerow({
                "AN8": "9999", "ALPH": "No Date Record", "AT1": "C",
                "PH1": "09171234567", "ADD1": "Street X", "ADD2": "",
                "CTY1": "Manila", "ADDS": "00", "ADDZ": "1000",
                "COUN": "PHL", "TAX": "999999999000", "PA8": "0",
                "UPMJ": "", "UPMT": "",
            })
        e = MockExtractor(file_path=csv_path)
        records = e.extract(last_upmj=126072, last_upmt=28800)
        assert len(records) == 1
        