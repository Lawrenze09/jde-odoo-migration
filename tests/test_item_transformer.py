"""
tests/test_item_transformer.py

Tests for ItemTransformer — field mapping, normalization, and STKT behavior.
Pure unit tests — no external dependencies, no Odoo connection required.
"""

import pytest
from transformers.item_transformer import ItemTransformer, STKT_TO_ODOO_TYPE, STKT_BEHAVIOR


@pytest.fixture
def transformer():
    return ItemTransformer()


@pytest.fixture
def valid_record():
    """A complete raw F4101 record with all fields populated."""
    return {
        "ITM":  "2002",
        "DSC1": "Gaisano Slippers Basic",
        "DSC2": "",
        "STKT": "S",
        "UOM1": "ea",     # lowercase — transformer must uppercase
        "UOM2": "ea",
        "SRP1": "199.00",
        "UPMJ": "126070",
        "UPMT": "14400",
    }


class TestITMNormalization:
    def test_itm_converts_to_integer(self, transformer, valid_record):
        result = transformer._transform_one(valid_record)
        assert result["_jde_itm"] == 2002

    def test_itm_none_returns_none(self, transformer, valid_record):
        valid_record["ITM"] = None
        result = transformer._transform_one(valid_record)
        assert result["_jde_itm"] is None

    def test_itm_empty_string_returns_none(self, transformer, valid_record):
        valid_record["ITM"] = ""
        result = transformer._transform_one(valid_record)
        assert result["_jde_itm"] is None

    def test_default_code_is_string_of_itm(self, transformer, valid_record):
        """default_code must be the ITM as a string — Odoo idempotency key."""
        result = transformer._transform_one(valid_record)
        assert result["default_code"] == "2002"


class TestSTKTMapping:
    def test_stkt_s_maps_to_product(self, transformer, valid_record):
        valid_record["STKT"] = "S"
        result = transformer._transform_one(valid_record)
        assert result["type"] == "consu"

    def test_stkt_n_maps_to_consu(self, transformer, valid_record):
        valid_record["STKT"] = "N"
        result = transformer._transform_one(valid_record)
        assert result["type"] == "consu"

    def test_stkt_o_maps_to_service(self, transformer, valid_record):
        valid_record["STKT"] = "O"
        result = transformer._transform_one(valid_record)
        assert result["type"] == "service"

    def test_invalid_stkt_maps_to_none(self, transformer, valid_record):
        """Unknown STKT passes through as None — validator catches it."""
        valid_record["STKT"] = "Z"
        result = transformer._transform_one(valid_record)
        assert result["type"] is None

    def test_stkt_lowercased_input_normalized(self, transformer, valid_record):
        """Lowercase STKT must be uppercased before mapping."""
        valid_record["STKT"] = "s"
        result = transformer._transform_one(valid_record)
        assert result["type"] == "consu"


class TestBehaviorFlags:
    def test_stkt_s_is_both_sale_and_purchase(self, transformer, valid_record):
        valid_record["STKT"] = "S"
        result = transformer._transform_one(valid_record)
        assert result["sale_ok"] is True
        assert result["purchase_ok"] is True

    def test_stkt_n_is_both_sale_and_purchase(self, transformer, valid_record):
        valid_record["STKT"] = "N"
        result = transformer._transform_one(valid_record)
        assert result["sale_ok"] is True
        assert result["purchase_ok"] is True

    def test_stkt_o_is_not_sale_ok(self, transformer, valid_record):
        """Outside operations are purchasable but NOT directly sellable."""
        valid_record["STKT"] = "O"
        result = transformer._transform_one(valid_record)
        assert result["sale_ok"] is False
        assert result["purchase_ok"] is True


class TestUOMNormalization:
    def test_uom1_uppercased(self, transformer, valid_record):
        valid_record["UOM1"] = "ea"
        result = transformer._transform_one(valid_record)
        assert result["_jde_uom1"] == "EA"
        assert result["uom_id"] == "EA"

    def test_uom2_uppercased(self, transformer, valid_record):
        valid_record["UOM2"] = "kg"
        result = transformer._transform_one(valid_record)
        assert result["_jde_uom2"] == "KG"
        assert result["uom_po_id"] == "KG"

    def test_empty_uom1_returns_none(self, transformer, valid_record):
        valid_record["UOM1"] = ""
        result = transformer._transform_one(valid_record)
        assert result["uom_id"] is None

    def test_empty_uom2_returns_none(self, transformer, valid_record):
        valid_record["UOM2"] = ""
        result = transformer._transform_one(valid_record)
        assert result["uom_po_id"] is None


class TestPriceNormalization:
    def test_valid_price_converts_to_float(self, transformer, valid_record):
        valid_record["SRP1"] = "450.00"
        result = transformer._transform_one(valid_record)
        assert result["list_price"] == 450.0

    def test_empty_price_returns_none(self, transformer, valid_record):
        """Empty price must return None — not 0.0 (which means free product)."""
        valid_record["SRP1"] = ""
        result = transformer._transform_one(valid_record)
        assert result["list_price"] is None

    def test_none_price_returns_none(self, transformer, valid_record):
        valid_record["SRP1"] = None
        result = transformer._transform_one(valid_record)
        assert result["list_price"] is None

    def test_negative_price_passes_through(self, transformer, valid_record):
        """Negative prices are the validator's concern — transformer passes them through."""
        valid_record["SRP1"] = "-100.00"
        result = transformer._transform_one(valid_record)
        assert result["list_price"] == -100.0

    def test_invalid_price_returns_none(self, transformer, valid_record):
        valid_record["SRP1"] = "abc"
        result = transformer._transform_one(valid_record)
        assert result["list_price"] is None

    def test_zero_price_returns_zero(self, transformer, valid_record):
        """Explicit 0.00 is a valid price — free product is intentional here."""
        valid_record["SRP1"] = "0.00"
        result = transformer._transform_one(valid_record)
        assert result["list_price"] == 0.0


class TestAuditComment:
    def test_comment_contains_itm(self, transformer, valid_record):
        result = transformer._transform_one(valid_record)
        assert "ITM=2002" in result["comment"]

    def test_comment_contains_date(self, transformer, valid_record):
        """UPMJ=126070 should convert to a recognizable date string."""
        result = transformer._transform_one(valid_record)
        assert "2026" in result["comment"]

    def test_comment_none_upmj_is_safe(self, transformer, valid_record):
        valid_record["UPMJ"] = None
        result = transformer._transform_one(valid_record)
        assert "unknown" in result["comment"]
        