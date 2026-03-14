"""
tests/test_transformer.py

Tests for CustomerTransformer field mapping and conversion logic.
No mocking required — transformer has no external dependencies.
Each test provides a minimal input dict and asserts one specific behavior.
"""

import pytest
from transformers.customer_transformer import CustomerTransformer


@pytest.fixture
def transformer():
    """Return a fresh CustomerTransformer instance for each test."""
    return CustomerTransformer()


@pytest.fixture
def valid_raw_record():
    """
    A complete, valid raw JDE record.
    Used as a base — individual tests override specific fields.
    """
    return {
        "AN8": "1001",
        "ALPH": "Limketkai Center Inc",
        "AT1": "C",
        "PH1": "+63 82 234 5678",
        "ADD1": "Limketkai Drive",
        "ADD2": None,
        "CTY1": "Cagayan de Oro",
        "ADDS": "10",
        "ADDZ": "9000",
        "COUN": "PHL",
        "TAX": "123456789000",
        "PA8": "0",
        "UPMJ": "126072",
        "UPMT": "28800",
    }


class TestAN8Transformation:
    def test_an8_converts_to_integer(self, transformer, valid_raw_record):
        """AN8 string '1001' must become integer 1001 in _jde_an8."""
        result = transformer.transform(valid_raw_record)
        assert result["_jde_an8"] == 1001
        assert isinstance(result["_jde_an8"], int)

    def test_an8_none_returns_none(self, transformer, valid_raw_record):
        """AN8 of None must produce _jde_an8 of None without crashing."""
        valid_raw_record["AN8"] = None
        result = transformer.transform(valid_raw_record)
        assert result["_jde_an8"] is None


class TestCustomerRankMapping:
    def test_customer_type_c_maps_to_rank_1(self, transformer, valid_raw_record):
        """AT1='C' must produce customer_rank=1 — this record is a customer."""
        valid_raw_record["AT1"] = "C"
        result = transformer.transform(valid_raw_record)
        assert result["customer_rank"] == 1

    def test_vendor_type_v_maps_to_rank_0(self, transformer, valid_raw_record):
        """AT1='V' must produce customer_rank=0 — vendors are not customers."""
        valid_raw_record["AT1"] = "V"
        result = transformer.transform(valid_raw_record)
        assert result["customer_rank"] == 0

    def test_invalid_type_maps_to_rank_0(self, transformer, valid_raw_record):
        """AT1='X' must produce customer_rank=0 — validator catches it later."""
        valid_raw_record["AT1"] = "X"
        result = transformer.transform(valid_raw_record)
        assert result["customer_rank"] == 0

    def test_raw_at1_preserved_for_validator(self, transformer, valid_raw_record):
        """_jde_at1 must contain the raw AT1 value so validator Rule 4 can check it."""
        valid_raw_record["AT1"] = "X"
        result = transformer.transform(valid_raw_record)
        assert result["_jde_at1"] == "X"


class TestPhoneNormalization:
    def test_spaces_removed_from_phone(self, transformer, valid_raw_record):
        """Spaces in PH1 must be stripped — '+63 82 234 5678' becomes '+63822345678'."""
        valid_raw_record["PH1"] = "+63 82 234 5678"
        result = transformer.transform(valid_raw_record)
        assert result["phone"] == "+63822345678"

    def test_dashes_removed_from_phone(self, transformer, valid_raw_record):
        """Dashes in PH1 must be stripped — '02-8525-4300' becomes '0285254300'."""
        valid_raw_record["PH1"] = "02-8525-4300"
        result = transformer.transform(valid_raw_record)
        assert result["phone"] == "0285254300"

    def test_none_phone_returns_none(self, transformer, valid_raw_record):
        """None PH1 must return None phone — validator catches missing phone."""
        valid_raw_record["PH1"] = None
        result = transformer.transform(valid_raw_record)
        assert result["phone"] is None


class TestJulianDateConversion:
    def test_julian_126072_converts_to_march_13_2026(self, transformer):
        """126072 must convert to March 13, 2026 — validates the core formula."""
        date = transformer._julian_to_date("126072")
        assert date.year == 2026
        assert date.month == 3
        assert date.day == 13

    def test_julian_none_returns_none(self, transformer):
        """None Julian value must return None without crashing."""
        assert transformer._julian_to_date(None) is None

    def test_audit_comment_contains_date(self, transformer, valid_raw_record):
        """Comment field must contain the converted JDE last-updated date."""
        result = transformer.transform(valid_raw_record)
        assert "2026-03-13" in result["comment"]
        assert "AN8=1001" in result["comment"]


class TestParentHierarchy:
    def test_pa8_zero_returns_none(self, transformer, valid_raw_record):
        """PA8='0' means no parent in JDE — must map to None, not '0'."""
        valid_raw_record["PA8"] = "0"
        result = transformer.transform(valid_raw_record)
        assert result["parent_an8"] is None

    def test_pa8_nonzero_preserved(self, transformer, valid_raw_record):
        """PA8 with a real value must be preserved for Phase 2 parent resolution."""
        valid_raw_record["PA8"] = "1004"
        result = transformer.transform(valid_raw_record)
        assert result["parent_an8"] == "1004"
        