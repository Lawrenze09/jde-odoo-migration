"""
extractors/mock_extractor.py

Role in pipeline: Development-stage extractor for the ETL pipeline.
Reads raw JDE F0101 customer data from a local CSV file instead of
connecting to a live Oracle JDE database. Used for development 
and testing. When running against a real Oracle JDE database, 
JdeExtractor replaces this via --source oracle.

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

    def extract(self) -> list[dict]:
        """
        Read all records from the CSV file and return as list of dicts.

        Replaces pandas NaN values with None before returning — NaN is
        a pandas-specific float value that would cause type errors in
        the transformer and validator. None is Python's standard null.

        Returns:
            list[dict]: Raw JDE records. Each dict key is a JDE column
                        name (AN8, ALPH, AT1, etc.), values are unmodified.

        Raises:
            FileNotFoundError: If the CSV file does not exist at file_path.
            Exception: Logs and re-raises any unexpected read error.
        """
        logger.info(f"Extracting records from {self.file_path}")

        try:
            # read_csv returns a DataFrame — a table structure with
            # typed columns. dtype=str prevents pandas from mangling
            # values like phone numbers or zip codes that look numeric.
            df = pd.read_csv(self.file_path, dtype=str)

            # Replace NaN with None throughout the entire DataFrame.
            # pandas uses NaN (float) for empty cells. Python validators
            # check for None — not NaN. Without this, empty cells pass
            # None checks and cause silent failures downstream.
            records = df.to_dict(orient="records")

            # NaN cleanup — pandas uses float NaN for empty cells even with dtype=str.
            # Validators check for None, not NaN. Convert here so nothing slips through.
            records = [
                {k: None if (isinstance(v, float) and pd.isna(v)) else v
                 for k, v in record.items()}
                for record in records
            ]

            logger.info(f"Extracted {len(records)} records successfully")
            return records

        except FileNotFoundError:
            logger.error(f"CSV file not found: {self.file_path}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during extraction: {e}")
            raise
        