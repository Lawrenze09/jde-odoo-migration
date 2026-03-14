"""
loaders/csv_loader.py

Role in pipeline: Dry-run output loader — used when DRY_RUN=true.
Instead of writing records to Odoo, writes them to a timestamped CSV
file in the output/ folder. Gives business owners and developers a
preview of exactly what would be loaded into Odoo before committing.

Input:  list[dict] of valid records from CustomerValidator
Output: output/dry_run_YYYYMMDD_HHMMSS.csv
"""

import csv
import os
from datetime import datetime
from utils.logger import get_logger

logger = get_logger(__name__)

# Odoo res.partner fields to include in the dry run output.
# Internal keys prefixed with _ are excluded — they are pipeline metadata,
# not Odoo fields. Business owners should not see _jde_an8 or _jde_at1.
ODOO_OUTPUT_FIELDS = [
    "name",
    "phone",
    "street",
    "street2",
    "city",
    "zip",
    "state_code",
    "country_code",
    "vat",
    "customer_rank",
    "is_company",
    "parent_an8",
    "comment",
]


class CsvLoader:
    """
    Writes valid transformed records to a CSV file for dry run preview.
    Each run produces a uniquely timestamped file — previous dry runs
    are never overwritten, giving a history of pipeline previews.
    """

    def __init__(self, output_dir: str = "output"):
        """
        Initialize the CsvLoader with an output directory.

        Args:
            output_dir (str): Directory to write dry run files.
                              Created automatically if it does not exist.
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"CsvLoader initialized | output_dir: {output_dir}")

    def load(self, valid_records: list[dict]) -> str:
        """
        Write valid records to a timestamped CSV file.

        Excludes internal pipeline keys (prefixed with _) from output.
        Column order follows ODOO_OUTPUT_FIELDS for readability.

        Args:
            valid_records (list[dict]): Valid transformed records from
                                        CustomerValidator.validate_batch()

        Returns:
            str: Full path to the generated CSV file.

        Raises:
            Exception: Logs and re-raises any file write error.
        """
        if not valid_records:
            logger.warning("No valid records to write — dry run CSV not created")
            return None

        output_path = self._build_output_path()

        try:
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=ODOO_OUTPUT_FIELDS,
                    extrasaction="ignore",  # silently drops _ prefixed keys
                )
                writer.writeheader()
                writer.writerows(valid_records)

            logger.info(
                f"Dry run CSV written | "
                f"records: {len(valid_records)} | "
                f"path: {output_path}"
            )
            return output_path

        except Exception as e:
            logger.error(f"Failed to write dry run CSV: {e}")
            raise

    def load_failed(self, failed_records: list[dict]) -> str:
        """
        Write failed records to a separate timestamped CSV file.
        Includes the failure rule and reason so reviewers know why
        each record was rejected.

        Args:
            failed_records (list[dict]): Failed records from
                                         CustomerValidator.validate_batch()
                                         Each record includes _failed_rule
                                         and _failure_reason keys.

        Returns:
            str: Full path to the generated failed records CSV file.
        """
        if not failed_records:
            logger.info("No failed records — skipping failed CSV")
            return None

        output_path = self._build_output_path(prefix="failed")

        # Failed record output includes all Odoo fields plus failure metadata
        failed_fields = ODOO_OUTPUT_FIELDS + ["_jde_an8", "_failed_rule", "_failure_reason"]

        try:
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=failed_fields,
                    extrasaction="ignore",
                )
                writer.writeheader()
                writer.writerows(failed_records)

            logger.info(
                f"Failed records CSV written | "
                f"records: {len(failed_records)} | "
                f"path: {output_path}"
            )
            return output_path

        except Exception as e:
            logger.error(f"Failed to write failed records CSV: {e}")
            raise

    def _build_output_path(self, prefix: str = "dry_run") -> str:
        """
        Build a timestamped output file path.

        Args:
            prefix (str): File name prefix. Default 'dry_run'.
                          Use 'failed' for failed records output.

        Returns:
            str: Full file path e.g. output/dry_run_20260313_201500.csv
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{prefix}_{timestamp}.csv"
        return os.path.join(self.output_dir, filename)
    