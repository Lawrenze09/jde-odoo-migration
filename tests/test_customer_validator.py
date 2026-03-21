"""
tests/test_validator.py

Tests for CustomerValidator — one test per business rule plus edge cases
for the duplicate pre-scan logic. No mocking required — validator has
no external dependencies. Each test constructs minimal transformed records
that match the shape CustomerTransformer.transform() produces.
"""

import pytest
from validators.customer_validator import CustomerValidator


@pytest.fixture
def validator():
    """Return a fresh CustomerValidator instance for each test."""
    return CustomerValidator()


@pytest.fixture
def valid_transformed_record():
    """
    A complete, valid transformed record — the shape CustomerTransformer produces.
    Used as a base — individual tests override specific fields to trigger rules.
    """
    return {
        "_jde_an8": 1001,
        "_jde_at1": "C",
        "name": "Limketkai Center Inc",
        "is_company": True,
        "customer_rank": 1,
        "phone": "+63822345678",
        "street": "Limketkai Drive",
        "street2": None,
        "city": "Cagayan de Oro",
        "zip": "9000",
        "state_code": "10",
        "country_code": "PHL",
        "vat": "123456789000",
        "parent_an8": None,
        "comment": "Migrated from JDE F0101 | AN8=1001 | JDE last updated: 2026-03-13",
    }


class TestRule01AN8Required:
    def test_missing_an8_fails(self, validator, valid_transformed_record):
        """Rule 1 — record with AN8=None must fail validation."""
        valid_transformed_record["_jde_an8"] = None
        valid, failed = validator.validate_batch([valid_transformed_record])
        assert len(failed) == 1
        assert failed[0]["_failed_rule"] == "RULE_01_AN8_REQUIRED"

    def test_valid_an8_passes(self, validator, valid_transformed_record):
        """Rule 1 — record with valid AN8 must pass Rule 1."""
        valid, failed = validator.validate_batch([valid_transformed_record])
        assert len(valid) == 1


class TestRule02AN8Duplicate:
    def test_duplicate_an8_both_fail(self, validator, valid_transformed_record):
        """Rule 2 — both records with the same AN8 must fail, not just the second."""
        record_a = valid_transformed_record.copy()
        record_b = valid_transformed_record.copy()
        record_b["name"] = "Duplicate Record Test"

        valid, failed = validator.validate_batch([record_a, record_b])
        assert len(failed) == 2
        assert len(valid) == 0
        assert all(f["_failed_rule"] == "RULE_02_AN8_DUPLICATE" for f in failed)

    def test_unique_an8_passes(self, validator, valid_transformed_record):
        """Rule 2 — records with different AN8 values must both pass."""
        record_a = valid_transformed_record.copy()
        record_b = valid_transformed_record.copy()
        record_b["_jde_an8"] = 1002
        record_b["phone"] = "09171234567"

        valid, failed = validator.validate_batch([record_a, record_b])
        assert len(valid) == 2
        assert len(failed) == 0


class TestRule03NameRequired:
    def test_missing_name_fails(self, validator, valid_transformed_record):
        """Rule 3 — record with name=None must fail validation."""
        valid_transformed_record["name"] = None
        valid, failed = validator.validate_batch([valid_transformed_record])
        assert len(failed) == 1
        assert failed[0]["_failed_rule"] == "RULE_03_NAME_REQUIRED"

    def test_empty_string_name_fails(self, validator, valid_transformed_record):
        """Rule 3 — empty string name must also fail — transformer returns None for empty."""
        valid_transformed_record["name"] = ""
        valid, failed = validator.validate_batch([valid_transformed_record])
        assert len(failed) == 1
        assert failed[0]["_failed_rule"] == "RULE_03_NAME_REQUIRED"


class TestRule04AddressType:
    def test_invalid_at1_fails(self, validator, valid_transformed_record):
        """Rule 4 — AT1='X' must fail with RULE_04_INVALID_ADDRESS_TYPE."""
        valid_transformed_record["_jde_at1"] = "X"
        valid, failed = validator.validate_batch([valid_transformed_record])
        assert len(failed) == 1
        assert failed[0]["_failed_rule"] == "RULE_04_INVALID_ADDRESS_TYPE"

    def test_valid_at1_c_passes(self, validator, valid_transformed_record):
        """Rule 4 — AT1='C' must pass Rule 4."""
        valid_transformed_record["_jde_at1"] = "C"
        valid, failed = validator.validate_batch([valid_transformed_record])
        assert len(valid) == 1

    def test_valid_at1_v_passes(self, validator, valid_transformed_record):
        """Rule 4 — AT1='V' must pass Rule 4 — vendor is a valid address type."""
        valid_transformed_record["_jde_at1"] = "V"
        valid, failed = validator.validate_batch([valid_transformed_record])
        assert len(valid) == 1


class TestRule05Phone:
    def test_missing_phone_fails(self, validator, valid_transformed_record):
        """Rule 5 — phone=None must fail with RULE_05_PHONE_MISSING."""
        valid_transformed_record["phone"] = None
        valid, failed = validator.validate_batch([valid_transformed_record])
        assert len(failed) == 1
        assert failed[0]["_failed_rule"] == "RULE_05_PHONE_MISSING"

    def test_invalid_phone_format_fails(self, validator, valid_transformed_record):
        """Rule 5 — '12345' must fail with RULE_05_PHONE_FORMAT."""
        valid_transformed_record["phone"] = "12345"
        valid, failed = validator.validate_batch([valid_transformed_record])
        assert len(failed) == 1
        assert failed[0]["_failed_rule"] == "RULE_05_PHONE_FORMAT"

    def test_ph_mobile_format_passes(self, validator, valid_transformed_record):
        """Rule 5 — Philippine mobile 09XXXXXXXXX must pass."""
        valid_transformed_record["phone"] = "09171234567"
        valid, failed = validator.validate_batch([valid_transformed_record])
        assert len(valid) == 1

    def test_ph_landline_format_passes(self, validator, valid_transformed_record):
        """Rule 5 — Philippine landline +63XXXXXXXXX must pass."""
        valid_transformed_record["phone"] = "+63822345678"
        valid, failed = validator.validate_batch([valid_transformed_record])
        assert len(valid) == 1


class TestRule06StreetRequired:
    def test_missing_street_fails(self, validator, valid_transformed_record):
        """Rule 6 — street=None must fail with RULE_06_ADDRESS_REQUIRED."""
        valid_transformed_record["street"] = None
        valid, failed = validator.validate_batch([valid_transformed_record])
        assert len(failed) == 1
        assert failed[0]["_failed_rule"] == "RULE_06_ADDRESS_REQUIRED"


class TestRule07CityRequired:
    def test_missing_city_fails(self, validator, valid_transformed_record):
        """Rule 7 — city=None must fail with RULE_07_CITY_REQUIRED."""
        valid_transformed_record["city"] = None
        valid, failed = validator.validate_batch([valid_transformed_record])
        assert len(failed) == 1
        assert failed[0]["_failed_rule"] == "RULE_07_CITY_REQUIRED"


class TestRule08ZipFormat:
    def test_non_numeric_zip_fails(self, validator, valid_transformed_record):
        """Rule 8 — zip='ABC123' must fail with RULE_08_ZIP_FORMAT."""
        valid_transformed_record["zip"] = "ABC123"
        valid, failed = validator.validate_batch([valid_transformed_record])
        assert len(failed) == 1
        assert failed[0]["_failed_rule"] == "RULE_08_ZIP_FORMAT"

    def test_numeric_zip_passes(self, validator, valid_transformed_record):
        """Rule 8 — zip='9000' must pass."""
        valid_transformed_record["zip"] = "9000"
        valid, failed = validator.validate_batch([valid_transformed_record])
        assert len(valid) == 1

    def test_none_zip_passes(self, validator, valid_transformed_record):
        """Rule 8 — zip=None must pass — zip is optional."""
        valid_transformed_record["zip"] = None
        valid, failed = validator.validate_batch([valid_transformed_record])
        assert len(valid) == 1
        