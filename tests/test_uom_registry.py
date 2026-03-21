"""
tests/test_uom_registry.py

Tests for UomRegistry — startup validation, resolution, and error handling.
Uses mock Odoo XML-RPC responses and a temporary CSV so tests never
require a live Odoo connection or touch real config files.

Mock data reflects Odoo saas-19.2 structure:
    - No category_id field on uom.uom records
    - Category sourced from mapping CSV instead
"""

import pytest
import csv
from unittest.mock import MagicMock
from loaders.uom_registry import UomRegistry, UomRecord, SERVICE_COMPATIBLE_CATEGORIES


def make_mock_models(uom_records: list[dict]):
    """
    Build a mock Odoo XML-RPC models proxy that returns the given UOM records.
    Records must have 'id' and 'name' only — no category_id in saas-19.2.
    """
    models = MagicMock()
    models.execute_kw.return_value = uom_records
    return models


def make_mapping_csv(tmp_path, rows: list[dict]) -> str:
    """
    Write a temporary uom_mapping.csv with jde_code, odoo_name, category columns.
    Returns the file path.
    """
    path = str(tmp_path / "uom_mapping.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["jde_code", "odoo_name", "category"])
        writer.writeheader()
        writer.writerows(rows)
    return path


# ── Mock data — reflects actual Odoo saas-19.2 uom.uom structure ────────────
# No category_id — removed from XML-RPC API in saas-19.2.
# Category is owned by the mapping CSV.
STANDARD_ODOO_UOMS = [
    {"id": 1,  "name": "Units"},
    {"id": 2,  "name": "Dozen(s)"},   # kept for test coverage
    {"id": 3,  "name": "kg"},
    {"id": 4,  "name": "lb"},         # kept for test coverage
    {"id": 5,  "name": "L"},
    {"id": 6,  "name": "Hours"},
]

# Mapping includes DZ and LB for test coverage of category validation.
# Real config/uom_mapping.csv only maps UOMs that exist in production Odoo.
STANDARD_MAPPING = [
    {"jde_code": "EA",  "odoo_name": "Units",    "category": "Unit"},
    {"jde_code": "CS",  "odoo_name": "Units",    "category": "Unit"},
    {"jde_code": "DZ",  "odoo_name": "Dozen(s)", "category": "Unit"},
    {"jde_code": "KG",  "odoo_name": "kg",       "category": "Weight"},
    {"jde_code": "LB",  "odoo_name": "lb",       "category": "Weight"},
    {"jde_code": "L",   "odoo_name": "L",        "category": "Volume"},
    {"jde_code": "HR",  "odoo_name": "Hours",    "category": "Time"},
]


@pytest.fixture
def registry(tmp_path):
    """Return a fully initialized UomRegistry with standard test data."""
    mapping_path = make_mapping_csv(tmp_path, STANDARD_MAPPING)
    models       = make_mock_models(STANDARD_ODOO_UOMS)
    return UomRegistry(models, uid=2, password="test", db="test", mapping_path=mapping_path)


class TestRegistryInitialization:
    def test_registry_builds_successfully(self, registry):
        """Registry initializes without error given valid mapping and Odoo data."""
        assert registry is not None

    def test_all_mapped_codes_are_resolvable(self, registry):
        """Every JDE code in the mapping must resolve after init."""
        for code in ["EA", "CS", "DZ", "KG", "LB", "L", "HR"]:
            assert registry.is_resolvable(code), f"{code} should be resolvable"

    def test_unknown_code_is_not_resolvable(self, registry):
        """Codes not in the mapping must not resolve."""
        assert not registry.is_resolvable("XX")
        assert not registry.is_resolvable("ZZ")

    def test_missing_mapping_file_raises_file_not_found(self, tmp_path):
        """Missing CSV file must raise FileNotFoundError at startup — fail fast."""
        models = make_mock_models(STANDARD_ODOO_UOMS)
        with pytest.raises(FileNotFoundError):
            UomRegistry(
                models, uid=2, password="test", db="test",
                mapping_path=str(tmp_path / "nonexistent.csv")
            )

    def test_invalid_odoo_name_raises_value_error(self, tmp_path):
        """
        Mapped Odoo name not in live Odoo must raise ValueError at startup.
        This is the fail-fast behavior that prevents partial runs.
        """
        bad_mapping = [{"jde_code": "EA", "odoo_name": "Does Not Exist", "category": "Unit"}]
        mapping_path = make_mapping_csv(tmp_path, bad_mapping)
        models       = make_mock_models(STANDARD_ODOO_UOMS)
        with pytest.raises(ValueError, match="unresolvable"):
            UomRegistry(models, uid=2, password="test", db="test", mapping_path=mapping_path)

    def test_missing_jde_code_column_raises_value_error(self, tmp_path):
        """CSV missing jde_code column must raise ValueError at startup."""
        path = str(tmp_path / "bad.csv")
        with open(path, "w", newline="") as f:
            f.write("wrong_column,odoo_name,category\nEA,Units,Unit\n")
        models = make_mock_models(STANDARD_ODOO_UOMS)
        with pytest.raises(ValueError, match="jde_code"):
            UomRegistry(models, uid=2, password="test", db="test", mapping_path=path)

    def test_missing_category_column_raises_value_error(self, tmp_path):
        """CSV missing category column must raise ValueError at startup."""
        path = str(tmp_path / "no_category.csv")
        with open(path, "w", newline="") as f:
            f.write("jde_code,odoo_name\nEA,Units\n")
        models = make_mock_models(STANDARD_ODOO_UOMS)
        with pytest.raises(ValueError, match="category"):
            UomRegistry(models, uid=2, password="test", db="test", mapping_path=path)


class TestResolution:
    def test_resolve_ea_returns_unit(self, registry):
        """EA must resolve to Units with correct id and category."""
        uom = registry.resolve("EA")
        assert isinstance(uom, UomRecord)
        assert uom.name == "Units"
        assert uom.category == "Unit"
        assert uom.id == 1

    def test_resolve_kg_returns_weight(self, registry):
        """KG must resolve to kg in the Weight category."""
        uom = registry.resolve("KG")
        assert uom.name == "kg"
        assert uom.category == "Weight"

    def test_resolve_hr_returns_time(self, registry):
        """HR must resolve to Hours in the Time category."""
        uom = registry.resolve("HR")
        assert uom.name == "Hours"
        assert uom.category == "Time"

    def test_resolve_unknown_code_raises_key_error(self, registry):
        """Resolving an unknown JDE code must raise KeyError."""
        with pytest.raises(KeyError):
            registry.resolve("XX")

    def test_cs_and_ea_resolve_to_same_odoo_uom(self, registry):
        """CS (case) and EA (each) both map to Units — same Odoo ID."""
        assert registry.resolve("CS").id == registry.resolve("EA").id

    def test_uom_record_is_frozen(self, registry):
        """UomRecord must be immutable — attempting mutation raises an error."""
        uom = registry.resolve("EA")
        with pytest.raises(Exception):
            uom.id = 999  # type: ignore


class TestCategoryInformation:
    def test_ea_and_lb_are_different_categories(self, registry):
        """EA (Unit) and LB (Weight) must be in different categories."""
        assert registry.resolve("EA").category != registry.resolve("LB").category

    def test_ea_and_dz_are_same_category(self, registry):
        """EA and DZ are both Unit category."""
        assert registry.resolve("EA").category == registry.resolve("DZ").category

    def test_kg_and_lb_are_same_category(self, registry):
        """KG and LB are both Weight category."""
        assert registry.resolve("KG").category == registry.resolve("LB").category

    def test_ea_and_kg_are_different_categories(self, registry):
        """EA (Unit) and KG (Weight) must be in different categories."""
        assert registry.resolve("EA").category != registry.resolve("KG").category

    def test_service_compatible_categories_contains_time(self):
        """Time must be in SERVICE_COMPATIBLE_CATEGORIES for Rule 09."""
        assert "Time" in SERVICE_COMPATIBLE_CATEGORIES

    def test_weight_not_in_service_compatible_categories(self):
        """Weight must NOT be service-compatible — catches Rule 09 violations."""
        assert "Weight" not in SERVICE_COMPATIBLE_CATEGORIES

    def test_volume_not_in_service_compatible_categories(self):
        """Volume must NOT be service-compatible."""
        assert "Volume" not in SERVICE_COMPATIBLE_CATEGORIES
        