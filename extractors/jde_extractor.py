"""
extractors/jde_extractor.py

Role in pipeline: Extracts records from a live Oracle JDE database
via python-oracledb. Replaces MockExtractor when --source oracle is used.

Supports two JDE tables:
    F0101 — Address Book Master (customers, vendors, contacts)
    F4101 — Item Master (products, services, materials)

JDE column naming convention:
    F0101 fields use 'AB' prefix: ABAN8, ABALPH, ABUPMJ, ABUPMT
    F4101 fields use 'IM' prefix: IMITM, IMDSC1, IMUPMJ, IMUPMT

Column aliases strip the prefix so downstream pipeline stages
(transformer, validator) receive the same field names as MockExtractor.
This means the rest of the pipeline is source-agnostic — only the
extractor changes between mock and oracle modes.

Watermark filtering:
    Full load  — last_upmj=0, last_upmt=0 → fetches all records
    Incremental — last_upmj/upmt from SyncLog → fetches only changed records

Pagination:
    Fetches in configurable page sizes (default 1000 rows) to avoid
    loading millions of rows into memory at once. MockExtractor loads
    everything — JdeExtractor pages through the result set.

Connection modes:
    Thin mode (default) — pure Python, no Oracle Client required
    Thick mode — requires Oracle Instant Client, enabled via thick_mode=True

Usage:
    extractor = JdeExtractor(table="customers")
    records   = extractor.extract()                        # full load
    records   = extractor.extract(last_upmj=126072)        # incremental

Not yet implemented — raises NotImplementedError until Oracle credentials
are configured in .env:
    ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE, ORACLE_USER, ORACLE_PASSWORD
"""

import os
from utils.logger import get_logger
from extractors.base_extractor import BaseExtractor

logger = get_logger(__name__)

# ── Standard JDE column mappings ────────────────────────────────────────────
# Each entry maps: alias (pipeline name) → JDE column name with prefix
# Aliases match MockExtractor output exactly — pipeline is source-agnostic.

F0101_COLUMNS = {
    "AN8":  "ABAN8",    # Address Number — primary key
    "ALPH": "ABALPH",   # Alpha name (company/person name)
    "AT1":  "ABAT1",    # Address type (C=Customer, V=Vendor, E=Employee)
    "PH1":  "ABPH1",    # Phone number 1
    "ADD1": "ABADD1",   # Address line 1
    "ADD2": "ABADD2",   # Address line 2
    "CTY1": "ABCTY1",   # City
    "ADDS": "ABADDS",   # State code
    "ADDZ": "ABADDZ",   # Postal/zip code
    "COUN": "ABCOUN",   # Country code
    "TAX":  "ABTAX",    # Tax ID
    "PA8":  "ABPA8",    # Parent address number
    "UPMJ": "ABUPMJ",   # Last updated date (Julian)
    "UPMT": "ABUPMT",   # Last updated time
}

F4101_COLUMNS = {
    "ITM":  "IMITM",    # Short item number — primary key
    "DSC1": "IMDSC1",   # Description line 1 (item name)
    "DSC2": "IMDSC2",   # Description line 2
    "STKT": "IMSTKT",   # Stocking type (S=Stocked, N=Non-stocked, O=Outside)
    "UOM1": "IMUOM1",   # Primary unit of measure
    "UOM2": "IMUOM2",   # Secondary/purchase unit of measure
    "SRP1": "IMSRP1",   # Suggested retail price
    "UPMJ": "IMUPMJ",   # Last updated date (Julian)
    "UPMT": "IMUPMT",   # Last updated time
}

# JDE Oracle schema — configurable per installation
# Most JDE environments use PRODDTA for production, TESTDTA for test
DEFAULT_SCHEMA    = "PRODDTA"
DEFAULT_PAGE_SIZE = 1000

# Map table name → column definitions and JDE table name
TABLE_CONFIG = {
    "customers": {
        "jde_table": "F0101",
        "columns":   F0101_COLUMNS,
        "upmj_col":  "ABUPMJ",
        "upmt_col":  "ABUPMT",
    },
    "items": {
        "jde_table": "F4101",
        "columns":   F4101_COLUMNS,
        "upmj_col":  "IMUPMJ",
        "upmt_col":  "IMUPMT",
    },
}


class JdeExtractor(BaseExtractor):
    """
    Extracts records from a live Oracle JDE database.
    Implements the same interface as MockExtractor — extract() returns
    list[dict] with field names matching mock CSV column headers.

    Not yet operational — requires Oracle credentials in .env:
        ORACLE_HOST     = jde-db.company.com
        ORACLE_PORT     = 1521
        ORACLE_SERVICE  = JDEDB
        ORACLE_USER     = JDE_READ
        ORACLE_PASSWORD = <password>
        ORACLE_SCHEMA   = PRODDTA  (optional, defaults to PRODDTA)
    """

    def __init__(
        self,
        table: str,
        schema: str | None = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        thick_mode: bool = False,
    ):
        """
        Initialize JdeExtractor for a specific JDE table.

        Args:
            table (str):      Which table to extract. 'customers' or 'items'.
            schema (str):     Oracle schema name. Defaults to ORACLE_SCHEMA
                              env var, then PRODDTA.
            page_size (int):  Rows per fetch. Default 1000. Increase for
                              faster extraction, decrease for lower memory.
            thick_mode (bool): Use Oracle Thick client mode. Requires
                               Oracle Instant Client installed.
                               Default False = Thin mode (pure Python).

        Raises:
            ValueError: If table is not 'customers' or 'items'.
        """
        if table not in TABLE_CONFIG:
            raise ValueError(
                f"Unknown table '{table}'. "
                f"Supported tables: {list(TABLE_CONFIG.keys())}"
            )

        self.table      = table
        self.config     = TABLE_CONFIG[table]
        self.schema     = schema or os.getenv("ORACLE_SCHEMA", DEFAULT_SCHEMA)
        self.page_size  = page_size
        self.thick_mode = thick_mode

        logger.info(
            f"JdeExtractor initialized | "
            f"table: {self.config['jde_table']} | "
            f"schema: {self.schema} | "
            f"page_size: {self.page_size} | "
            f"mode: {'thick' if thick_mode else 'thin'}"
        )

    def extract(
        self,
        last_upmj: int = 0,
        last_upmt: int = 0,
    ) -> list[dict]:
        """
        Extract records from JDE Oracle with watermark filtering.

        Full load:   extract()                    → all records
        Incremental: extract(last_upmj, last_upmt) → changed records only

        Args:
            last_upmj (int): Julian date watermark. 0 = full load.
            last_upmt (int): Time watermark. 0 = full load.

        Returns:
            list[dict]: Records with aliased field names matching
                        MockExtractor output format.

        Raises:
            NotImplementedError: Always — until Oracle credentials are
                                 configured and connection is tested.
            ImportError:         If python-oracledb is not installed.
        """
        self._check_credentials()
        self._check_oracledb_installed()

        # Connection and query logic goes here once credentials are available.
        # Architecture is designed — implementation follows credential setup.
        raise NotImplementedError(
            "JdeExtractor is not yet operational. "
            "Configure Oracle credentials in .env to enable:\n"
            "  ORACLE_HOST     = <hostname>\n"
            "  ORACLE_PORT     = 1521\n"
            "  ORACLE_SERVICE  = <service_name>\n"
            "  ORACLE_USER     = <read_only_user>\n"
            "  ORACLE_PASSWORD = <password>\n"
            "  ORACLE_SCHEMA   = PRODDTA\n"
            "Then connect to Oracle and remove this error."
        )

    def _check_credentials(self):
        """
        Verify all required Oracle credentials are present in environment.

        Raises:
            EnvironmentError: Lists all missing credentials so the operator
                              can fix everything at once, not one at a time.
        """
        required = {
            "ORACLE_HOST":     os.getenv("ORACLE_HOST"),
            "ORACLE_PORT":     os.getenv("ORACLE_PORT"),
            "ORACLE_SERVICE":  os.getenv("ORACLE_SERVICE"),
            "ORACLE_USER":     os.getenv("ORACLE_USER"),
            "ORACLE_PASSWORD": os.getenv("ORACLE_PASSWORD"),
        }

        missing = [key for key, value in required.items() if not value]

        if missing:
            raise EnvironmentError(
                f"Oracle credentials missing from .env: {missing}\n"
                f"Add them to .env to enable --source oracle."
            )

        logger.debug(
            f"Oracle credentials verified | "
            f"host: {required['ORACLE_HOST']} | "
            f"service: {required['ORACLE_SERVICE']} | "
            f"user: {required['ORACLE_USER']}"
        )

    def _check_oracledb_installed(self):
        """
        Verify python-oracledb is installed before attempting connection.

        Raises:
            ImportError: With installation instructions if not installed.
        """
        try:
            import oracledb  # noqa: F401
        except ImportError:
            raise ImportError(
                "python-oracledb is not installed. "
                "Run: python -m pip install oracledb\n"
                "Then configure Oracle credentials in .env."
            )

    def _build_query(self, last_upmj: int, last_upmt: int) -> tuple[str, dict]:
        """
        Build the parameterized SQL query for this table and watermark.

        Uses bind parameters (:last_upmj, :last_upmt) — never string
        interpolation — to prevent SQL injection and enable Oracle
        query plan caching.

        Args:
            last_upmj (int): Julian date watermark.
            last_upmt (int): Time watermark.

        Returns:
            tuple: (sql_string, bind_params_dict)
        """
        columns      = self.config["columns"]
        jde_table    = self.config["jde_table"]
        upmj_col     = self.config["upmj_col"]
        upmt_col     = self.config["upmt_col"]
        full_table   = f"{self.schema}.{jde_table}"

        # Build SELECT clause with aliases
        # e.g. ABAN8 AS AN8, ABALPH AS ALPH, ...
        select_parts = [
            f"{jde_col} AS {alias}"
            for alias, jde_col in columns.items()
        ]
        select_clause = ",\n       ".join(select_parts)

        if last_upmj == 0 and last_upmt == 0:
            # Full load — no watermark filter
            sql = f"""
                SELECT {select_clause}
                FROM   {full_table}
                ORDER BY {upmj_col}, {upmt_col}
            """
            params = {}
        else:
            # Incremental load — composite watermark filter
            # Same logic as MockExtractor._passes_watermark_filter()
            sql = f"""
                SELECT {select_clause}
                FROM   {full_table}
                WHERE  ({upmj_col} > :last_upmj)
                   OR  ({upmj_col} = :last_upmj AND {upmt_col} > :last_upmt)
                ORDER BY {upmj_col}, {upmt_col}
            """
            params = {
                "last_upmj": last_upmj,
                "last_upmt": last_upmt,
            }

        logger.debug(
            f"Query built | table: {full_table} | "
            f"watermark: UPMJ>{last_upmj} | mode: "
            f"{'incremental' if last_upmj > 0 else 'full load'}"
        )

        return sql.strip(), params

    def _fetch_pages(
        self,
        cursor,
        sql: str,
        params: dict,
    ) -> list[dict]:
        """
        Execute query and fetch results in pages to manage memory.

        Each page is converted to list[dict] immediately and appended
        to the result set. Column names come from cursor.description
        after stripping AS aliases — they already match pipeline names.

        Args:
            cursor: Oracle cursor from active connection.
            sql:    Parameterized SQL string.
            params: Bind parameter dict.

        Returns:
            list[dict]: All rows as dicts with aliased column names.
        """
        cursor.execute(sql, params)

        # Column names from cursor description — already aliased
        col_names = [col[0] for col in cursor.description]

        records = []
        page    = 0

        while True:
            rows = cursor.fetchmany(self.page_size)
            if not rows:
                break

            page += 1
            page_records = [
                {col: str(val) if val is not None else None
                 for col, val in zip(col_names, row)}
                for row in rows
            ]
            records.extend(page_records)

            logger.debug(
                f"Fetched page {page} | "
                f"rows: {len(rows)} | "
                f"total so far: {len(records)}"
            )

        logger.info(
            f"Extraction complete | "
            f"table: {self.config['jde_table']} | "
            f"total rows: {len(records)} | "
            f"pages: {page}"
        )

        return records
    