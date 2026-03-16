# extractors/mock_extractor.py
"""
extractors/mock_extractor.py

Role in pipeline: Development-stage extractor for the ETL pipeline.
Reads raw JDE F0101 customer data from a local CSV file instead of
connecting to a live Oracle JDE database. Used for development
and testing. When running against a real Oracle JDE database,
JdeExtractor replaces this via --source oracle.

Supports both full load and incremental sync via UPMJ+UPMT watermark.
    Full load:   extract()                          — returns all 20 records
    Incremental: extract(last_upmj=X, last_upmt=Y) — returns only newer records

Input:  CSV file path from settings.mock_data_path (default: mock_data/F0101.csv)
Output: list[dict] of raw JDE records with original column names as keys
"""

import pandas as pd
from extractors.base_extractor import BaseExtractor
from utils.logger import get_logger
from config.settings import get_settings

logger = get_logger(__name__)


class MockExtractor(BaseExtractor):
    """
    Reads JDE F0101 mock data from a CSV file.
    Implements the BaseExtractor contract — returns list[dict] with
    JDE column names as keys, same structure JdeExtractor will return.
    Supports incremental sync via UPMJ+UPMT watermark filtering.
    """

    def __init__(self, file_path: str = None):
        """
        Initialize the MockExtractor with an optional file path override.

        Args:
            file_path (str, optional): Path to the CSV file. Defaults to
                                       mock_data_path from settings.
        """
        settings = get_settings()
        # Allow override for testing — tests pass a specific file path
        # instead of relying on the .env setting
        self.file_path = file_path or settings.mock_data_path
        logger.info(f"MockExtractor initialized | source: {self.file_path}")

    def extract(
        self,
        last_upmj: int = 0,
        last_upmt: int = 0,
    ) -> list[dict]:
        """
        Read records from the CSV file and return as list of dicts.

        When last_upmj=0 and last_upmt=0, returns all records (full load).
        When watermark values are provided, applies UPMJ+UPMT filter and
        returns only records updated after that watermark point.

        Replaces pandas NaN values with None — NaN is a pandas-specific
        float that would cause type errors in the transformer and validator.

        Args:
            last_upmj (int): Julian date watermark from previous run.
                             0 means first run — return all records.
            last_upmt (int): Time in seconds watermark from previous run.
                             0 means first run — return all records.

        Returns:
            list[dict]: Raw JDE records. Each dict key is a JDE column
                        name (AN8, ALPH, AT1, etc.), values are unmodified.

        Raises:
            FileNotFoundError: If the CSV file does not exist at file_path.
            Exception: Logs and re-raises any unexpected read error.
        """
        is_incremental = last_upmj > 0
        mode = (
            f"incremental (UPMJ>{last_upmj}, UPMT>{last_upmt})"
            if is_incremental
            else "full load"
        )
        logger.info(f"Extracting records from {self.file_path} | mode: {mode}")

        try:
            # read_csv returns a DataFrame — dtype=str prevents pandas from
            # mangling phone numbers or zip codes that look numeric.
            df = pd.read_csv(self.file_path, dtype=str)

            records = df.to_dict(orient="records")

            # NaN cleanup — pandas uses float NaN for empty cells even with
            # dtype=str. Validators check for None, not NaN.
            records = [
                {
                    k: None if (isinstance(v, float) and pd.isna(v)) else v
                    for k, v in record.items()
                }
                for record in records
            ]

            # Apply incremental filter when watermark is provided.
            # last_upmj=0 means first run — skip filtering, return everything.
            if is_incremental:
                before_count = len(records)
                records = [
                    r for r in records
                    if self._passes_watermark_filter(r, last_upmj, last_upmt)
                ]
                logger.info(
                    f"Incremental filter applied | "
                    f"before: {before_count} | "
                    f"after: {len(records)} | "
                    f"skipped: {before_count - len(records)}"
                )

            logger.info(f"Extracted {len(records)} records successfully")
            return records

        except FileNotFoundError:
            logger.error(f"CSV file not found: {self.file_path}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during extraction: {e}")
            raise

    def _passes_watermark_filter(
        self,
        record: dict,
        last_upmj: int,
        last_upmt: int,
    ) -> bool:
        """
        Apply UPMJ+UPMT watermark filter to a single record.

        A record passes if it was updated strictly after the watermark:
            Condition 1: UPMJ > last_upmj  (newer date)
            Condition 2: UPMJ == last_upmj AND UPMT > last_upmt
                         (same date, later time)

        If UPMJ/UPMT cannot be parsed, the record is included — it is
        safer to process a record twice than to silently miss it.

        Args:
            record    (dict): Raw JDE record with UPMJ and UPMT keys.
            last_upmj  (int): Julian date watermark from previous run.
            last_upmt  (int): Time watermark in seconds from previous run.

        Returns:
            bool: True if this record should be included in this sync run.
        """
        try:
            raw_upmj = record.get("UPMJ")
            raw_upmt = record.get("UPMT")
            # Treat missing, empty string, or None as unparseable.
            # Include the record — safer to process twice than to miss it.
            if not raw_upmj or not raw_upmt:
                return True
            
            record_upmj = int(raw_upmj)
            record_upmt = int(raw_upmt)
            
        except (ValueError, TypeError):
            # Cannot parse — include record to avoid silent data loss.
            # Better to process a record twice than to miss an update.
            return True

        # Condition 1: record has a newer Julian date than the watermark
        if record_upmj > last_upmj:
            return True

        # Condition 2: same Julian date but updated later in the day.
        # This catches multiple updates on the same day after the watermark.
        if record_upmj == last_upmj and record_upmt > last_upmt:
            return True

        return False
    