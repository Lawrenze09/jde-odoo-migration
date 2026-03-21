"""
tests/test_jde_extractor.py

Tests for JdeExtractor — initialization, credential checking,
query building, and error handling.

These tests never connect to Oracle — they verify the extractor's
structure, error messages, and SQL generation logic in isolation.
"""

import pytest
import os
from unittest.mock import patch, MagicMock
from extractors.jde_extractor import (
    JdeExtractor, TABLE_CONFIG, F0101_COLUMNS, F4101_COLUMNS
)


class TestInitialization:
    def test_customers_table_accepted(self):
        extractor = JdeExtractor(table="customers")
        assert extractor.table == "customers"

    def test_items_table_accepted(self):
        extractor = JdeExtractor(table="items")
        assert extractor.table == "items"

    def test_invalid_table_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown table"):
            JdeExtractor(table="vendors")

    def test_default_schema_is_proddta(self):
        extractor = JdeExtractor(table="customers")
        assert extractor.schema == "PRODDTA"

    def test_custom_schema_is_used(self):
        extractor = JdeExtractor(table="customers", schema="TESTDTA")
        assert extractor.schema == "TESTDTA"

    def test_default_page_size(self):
        extractor = JdeExtractor(table="customers")
        assert extractor.page_size == 1000

    def test_custom_page_size(self):
        extractor = JdeExtractor(table="customers", page_size=500)
        assert extractor.page_size == 500

    def test_default_mode_is_thin(self):
        extractor = JdeExtractor(table="customers")
        assert extractor.thick_mode is False


class TestColumnMappings:
    def test_f0101_has_an8_column(self):
        assert "AN8" in F0101_COLUMNS
        assert F0101_COLUMNS["AN8"] == "ABAN8"

    def test_f0101_has_upmj_column(self):
        assert "UPMJ" in F0101_COLUMNS
        assert F0101_COLUMNS["UPMJ"] == "ABUPMJ"

    def test_f4101_has_itm_column(self):
        assert "ITM" in F4101_COLUMNS
        assert F4101_COLUMNS["ITM"] == "IMITM"

    def test_f4101_has_upmj_column(self):
        assert "UPMJ" in F4101_COLUMNS
        assert F4101_COLUMNS["UPMJ"] == "IMUPMJ"

    def test_f0101_aliases_match_mock_extractor_columns(self):
        """Aliases must match F0101.csv headers exactly."""
        expected = {"AN8", "ALPH", "AT1", "PH1", "ADD1", "ADD2",
                    "CTY1", "ADDS", "ADDZ", "COUN", "TAX", "PA8",
                    "UPMJ", "UPMT"}
        assert set(F0101_COLUMNS.keys()) == expected

    def test_f4101_aliases_match_mock_extractor_columns(self):
        """Aliases must match F4101.csv headers exactly."""
        expected = {"ITM", "DSC1", "DSC2", "STKT", "UOM1",
                    "UOM2", "SRP1", "UPMJ", "UPMT"}
        assert set(F4101_COLUMNS.keys()) == expected


class TestCredentialChecking:
    def test_missing_credentials_raises_environment_error(self):
        extractor = JdeExtractor(table="customers")
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(EnvironmentError, match="credentials missing"):
                extractor._check_credentials()

    def test_error_lists_all_missing_credentials(self):
        extractor = JdeExtractor(table="customers")
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(EnvironmentError) as exc:
                extractor._check_credentials()
            error_msg = str(exc.value)
            assert "ORACLE_HOST" in error_msg
            assert "ORACLE_USER" in error_msg
            assert "ORACLE_PASSWORD" in error_msg

    def test_all_credentials_present_passes(self):
        extractor = JdeExtractor(table="customers")
        env = {
            "ORACLE_HOST":     "jde-db.test.com",
            "ORACLE_PORT":     "1521",
            "ORACLE_SERVICE":  "JDEDB",
            "ORACLE_USER":     "JDE_READ",
            "ORACLE_PASSWORD": "secret",
        }
        with patch.dict(os.environ, env):
            extractor._check_credentials()  # must not raise


class TestOracledbCheck:
    def test_missing_oracledb_raises_import_error(self):
        extractor = JdeExtractor(table="customers")
        with patch.dict("sys.modules", {"oracledb": None}):
            with pytest.raises(ImportError, match="python-oracledb"):
                extractor._check_oracledb_installed()


class TestQueryBuilding:
    def test_full_load_has_no_where_clause(self):
        extractor = JdeExtractor(table="customers")
        sql, params = extractor._build_query(last_upmj=0, last_upmt=0)
        assert "WHERE" not in sql.upper()
        assert params == {}

    def test_incremental_load_has_where_clause(self):
        extractor = JdeExtractor(table="customers")
        sql, params = extractor._build_query(last_upmj=126072, last_upmt=28800)
        assert "WHERE" in sql.upper()
        assert params["last_upmj"] == 126072
        assert params["last_upmt"] == 28800

    def test_full_load_query_contains_all_f0101_aliases(self):
        extractor = JdeExtractor(table="customers")
        sql, _ = extractor._build_query(0, 0)
        for alias in F0101_COLUMNS:
            assert f"AS {alias}" in sql

    def test_full_load_query_contains_all_f4101_aliases(self):
        extractor = JdeExtractor(table="items")
        sql, _ = extractor._build_query(0, 0)
        for alias in F4101_COLUMNS:
            assert f"AS {alias}" in sql

    def test_query_uses_correct_schema(self):
        extractor = JdeExtractor(table="customers", schema="TESTDTA")
        sql, _ = extractor._build_query(0, 0)
        assert "TESTDTA.F0101" in sql

    def test_incremental_query_uses_bind_params_not_literals(self):
        """Must use :last_upmj not the actual value — prevents SQL injection."""
        extractor = JdeExtractor(table="customers")
        sql, _ = extractor._build_query(last_upmj=126072, last_upmt=28800)
        assert ":last_upmj" in sql
        assert ":last_upmt" in sql
        assert "126072" not in sql  # value must not be hardcoded in SQL

    def test_items_table_uses_im_prefix_columns(self):
        extractor = JdeExtractor(table="items")
        sql, _ = extractor._build_query(0, 0)
        assert "IMITM" in sql
        assert "IMDSC1" in sql

    def test_customers_table_uses_ab_prefix_columns(self):
        extractor = JdeExtractor(table="customers")
        sql, _ = extractor._build_query(0, 0)
        assert "ABAN8" in sql
        assert "ABALPH" in sql


class TestExtractRaisesNotImplemented:
    def test_extract_raises_not_implemented_without_credentials(self):
        """Extract must fail clearly until Oracle is configured."""
        extractor = JdeExtractor(table="customers")
        with pytest.raises((NotImplementedError, EnvironmentError, ImportError)):
            extractor.extract()
            