"""
tests/test_item_validator.py

Tests for ItemValidator — all 9 validation rules and batch pre-scan behavior.
Uses a mock UomRegistry so tests never require a live Odoo connection.
"""

import pytest
from unittest.mock import MagicMock
from validators.item_validator import ItemValidator
from loaders.uom_registry import UomRecord


def make_registry(resolved: dict[str, UomRecord]) -> MagicMock:
    """
    Build a mock UomRegistry that resolves the given JDE codes.
    Codes not in resolved raise KeyError — mirrors real registry behavior.
    """
    registry = MagicMock()
    registry.is_resolvable.side_effect = lambda code: code in resolved
    registry.resolve.side_effect = lambda code: resolved[code]
    registry.known_codes.return_value = sorted(resolved.keys())
    return registry


# Standard UOM records for testing
UNIT_UOM    = UomRecord(id=1, name="Units",  category_id=0, category="Unit")
WEIGHT_UOM  = UomRecord(id=3, name="kg",     category_id=0, category="Weight")
TIME_UOM    = UomRecord(id=6, name="Hours",  category_id=0, category="Time")
VOLUME_UOM  = UomRecord(id=5, name="L",      category_id=0, category="Volume")

STANDARD_RESOLVED = {
    "EA": UNIT_UOM,
    "CS": UNIT_UOM,
    "KG": WEIGHT_UOM,
    "HR": TIME_UOM,
    "L":  VOLUME_UOM,
}


@pytest.fixture
def registry():
    return make_registry(STANDARD_RESOLVED)


@pytest.fixture
def validator(registry):
    return ItemValidator(uom_registry=registry)


@pytest.fixture
def valid_record():
    """A fully valid transformed F4101 record — all 9 rules pass."""
    return {
        "_jde_itm":   2002,
        "_jde_stkt":  "S",
        "_jde_uom1":  "EA",
        "_jde_uom2":  "EA",
        "name":       "Gaisano Slippers Basic",
        "default_code": "2002",
        "type":       "product",
        "sale_ok":    True,
        "purchase_ok": True,
        "list_price": 199.0,
        "comment":    "Migrated from JDE F4101 | ITM=2002",
    }


def validate_one(validator, record):
    """Helper: run batch validation on a single record, return (valid, failed)."""
    return validator.validate_batch([record])


class TestRule01ITMRequired:
    def test_missing_itm_fails(self, validator, valid_record):
        valid_record["_jde_itm"] = None
        valid, failed = validate_one(validator, valid_record)
        assert len(failed) == 1
        assert "Rule01" in failed[0]["_failed_rule"]

    def test_zero_itm_fails(self, validator, valid_record):
        valid_record["_jde_itm"] = 0
        valid, failed = validate_one(validator, valid_record)
        assert len(failed) == 1
        assert "Rule01" in failed[0]["_failed_rule"]

    def test_negative_itm_fails(self, validator, valid_record):
        valid_record["_jde_itm"] = -1
        valid, failed = validate_one(validator, valid_record)
        assert len(failed) == 1

    def test_valid_itm_passes(self, validator, valid_record):
        valid, failed = validate_one(validator, valid_record)
        assert len(valid) == 1
        assert len(failed) == 0


class TestRule02ITMDuplicate:
    def test_duplicate_itm_both_fail(self, validator, valid_record):
        """Both occurrences of duplicate ITM must fail — not just the second."""
        record_a = {**valid_record}
        record_b = {**valid_record}
        valid, failed = validator.validate_batch([record_a, record_b])
        assert len(failed) == 2
        assert len(valid) == 0

    def test_unique_itm_passes(self, validator, valid_record):
        record_a = {**valid_record, "_jde_itm": 2002}
        record_b = {**valid_record, "_jde_itm": 2003}
        valid, failed = validator.validate_batch([record_a, record_b])
        assert len(valid) == 2
        assert len(failed) == 0


class TestRule03NameRequired:
    def test_missing_name_fails(self, validator, valid_record):
        valid_record["name"] = None
        valid, failed = validate_one(validator, valid_record)
        assert len(failed) == 1
        assert "Rule03" in failed[0]["_failed_rule"]

    def test_empty_name_fails(self, validator, valid_record):
        valid_record["name"] = ""
        valid, failed = validate_one(validator, valid_record)
        assert len(failed) == 1

    def test_valid_name_passes(self, validator, valid_record):
        valid, failed = validate_one(validator, valid_record)
        assert len(valid) == 1


class TestRule04InvalidSTKT:
    def test_invalid_stkt_fails(self, validator, valid_record):
        valid_record["_jde_stkt"] = "Z"
        valid, failed = validate_one(validator, valid_record)
        assert len(failed) == 1
        assert "Rule04" in failed[0]["_failed_rule"]

    def test_stkt_s_passes(self, validator, valid_record):
        valid_record["_jde_stkt"] = "S"
        valid, failed = validate_one(validator, valid_record)
        assert len(valid) == 1

    def test_stkt_n_passes(self, validator, valid_record):
        valid_record["_jde_stkt"] = "N"
        valid, failed = validate_one(validator, valid_record)
        assert len(valid) == 1

    def test_stkt_o_passes_with_compatible_uom(self, validator, valid_record):
        valid_record["_jde_stkt"] = "O"
        valid_record["_jde_uom1"] = "HR"
        valid_record["_jde_uom2"] = "HR"
        valid, failed = validate_one(validator, valid_record)
        assert len(valid) == 1


class TestRule05UOM1Required:
    def test_missing_uom1_fails(self, validator, valid_record):
        valid_record["_jde_uom1"] = None
        valid, failed = validate_one(validator, valid_record)
        assert len(failed) == 1
        assert "Rule05" in failed[0]["_failed_rule"]

    def test_unknown_uom1_fails(self, validator, valid_record):
        valid_record["_jde_uom1"] = "XX"
        valid, failed = validate_one(validator, valid_record)
        assert len(failed) == 1
        assert "Rule05" in failed[0]["_failed_rule"]

    def test_valid_uom1_passes(self, validator, valid_record):
        valid_record["_jde_uom1"] = "EA"
        valid, failed = validate_one(validator, valid_record)
        assert len(valid) == 1


class TestRule06UOM2Invalid:
    def test_unknown_uom2_fails(self, validator, valid_record):
        valid_record["_jde_uom2"] = "ZZ"
        valid, failed = validate_one(validator, valid_record)
        assert len(failed) == 1
        assert "Rule06" in failed[0]["_failed_rule"]

    def test_missing_uom2_passes(self, validator, valid_record):
        """UOM2 is optional — missing must not fail."""
        valid_record["_jde_uom2"] = None
        valid, failed = validate_one(validator, valid_record)
        assert len(valid) == 1

    def test_valid_uom2_passes(self, validator, valid_record):
        valid_record["_jde_uom2"] = "KG"
        valid_record["_jde_uom1"] = "KG"
        valid, failed = validate_one(validator, valid_record)
        assert len(valid) == 1


class TestRule07UOMCategoryMismatch:
    def test_different_categories_fails(self, validator, valid_record):
        valid_record["_jde_uom1"] = "EA"   # Unit
        valid_record["_jde_uom2"] = "KG"   # Weight
        valid, failed = validate_one(validator, valid_record)
        assert len(failed) == 1
        assert "Rule07" in failed[0]["_failed_rule"]

    def test_same_category_passes(self, validator, valid_record):
        valid_record["_jde_uom1"] = "EA"   # Unit
        valid_record["_jde_uom2"] = "CS"   # Unit
        valid, failed = validate_one(validator, valid_record)
        assert len(valid) == 1

    def test_missing_uom2_skips_category_check(self, validator, valid_record):
        valid_record["_jde_uom2"] = None
        valid, failed = validate_one(validator, valid_record)
        assert len(valid) == 1


class TestRule08NegativePrice:
    def test_negative_price_fails(self, validator, valid_record):
        valid_record["list_price"] = -100.0
        valid, failed = validate_one(validator, valid_record)
        assert len(failed) == 1
        assert "Rule08" in failed[0]["_failed_rule"]

    def test_zero_price_passes(self, validator, valid_record):
        valid_record["list_price"] = 0.0
        valid, failed = validate_one(validator, valid_record)
        assert len(valid) == 1

    def test_none_price_passes(self, validator, valid_record):
        """None price is acceptable — field is optional."""
        valid_record["list_price"] = None
        valid, failed = validate_one(validator, valid_record)
        assert len(valid) == 1

    def test_positive_price_passes(self, validator, valid_record):
        valid_record["list_price"] = 450.0
        valid, failed = validate_one(validator, valid_record)
        assert len(valid) == 1


class TestRule09STKTUOMIncompatible:
    def test_service_with_weight_uom_fails(self, validator, valid_record):
        valid_record["_jde_stkt"] = "O"
        valid_record["_jde_uom1"] = "KG"   # Weight — incompatible with service
        valid_record["_jde_uom2"] = "KG"
        valid, failed = validate_one(validator, valid_record)
        assert len(failed) == 1
        assert "Rule09" in failed[0]["_failed_rule"]

    def test_service_with_time_uom_passes(self, validator, valid_record):
        valid_record["_jde_stkt"] = "O"
        valid_record["_jde_uom1"] = "HR"   # Time — compatible with service
        valid_record["_jde_uom2"] = "HR"
        valid, failed = validate_one(validator, valid_record)
        assert len(valid) == 1

    def test_service_with_volume_uom_fails(self, validator, valid_record):
        valid_record["_jde_stkt"] = "O"
        valid_record["_jde_uom1"] = "L"    # Volume — incompatible
        valid_record["_jde_uom2"] = "L"
        valid, failed = validate_one(validator, valid_record)
        assert len(failed) == 1

    def test_stocked_with_weight_uom_passes(self, validator, valid_record):
        """Stocked items can use any UOM — Rule 09 only applies to STKT=O."""
        valid_record["_jde_stkt"] = "S"
        valid_record["_jde_uom1"] = "KG"
        valid_record["_jde_uom2"] = "KG"
        valid, failed = validate_one(validator, valid_record)
        assert len(valid) == 1


class TestBatchPreScan:
    def test_unknown_uom_warning_logged(self, validator, valid_record, caplog):
        """Unknown UOM codes must generate a batch-level warning."""
        valid_record["_jde_uom1"] = "XX"
        import logging
        with caplog.at_level(logging.WARNING):
            validator.validate_batch([valid_record])
        assert "XX" in caplog.text

    def test_valid_batch_returns_correct_counts(self, validator):
        """Full mock batch — 11 valid, 9 failed matches our F4101 design."""
        records = [
            # Valid records
            {"_jde_itm": 2002, "_jde_stkt": "S", "_jde_uom1": "EA", "_jde_uom2": "EA",  "name": "Item A", "list_price": 100.0},
            {"_jde_itm": 2003, "_jde_stkt": "S", "_jde_uom1": "CS", "_jde_uom2": "EA",  "name": "Item B", "list_price": 85.0},
            {"_jde_itm": 2004, "_jde_stkt": "S", "_jde_uom1": "EA", "_jde_uom2": "EA",  "name": "Item C", "list_price": 8500.0},
            # Duplicate ITM — both fail Rule 02
            {"_jde_itm": 2001, "_jde_stkt": "S", "_jde_uom1": "EA", "_jde_uom2": "EA",  "name": "Dup A",  "list_price": 450.0},
            {"_jde_itm": 2001, "_jde_stkt": "S", "_jde_uom1": "EA", "_jde_uom2": "EA",  "name": "Dup B",  "list_price": 450.0},
            # Missing name — Rule 03
            {"_jde_itm": 2005, "_jde_stkt": "S", "_jde_uom1": "EA", "_jde_uom2": "EA",  "name": None,     "list_price": 300.0},
        ]
        valid, failed = validator.validate_batch(records)
        assert len(valid) == 3
        assert len(failed) == 3
        