"""
transformers/customer_transformer.py

Role in pipeline: Second stage of the ETL pipeline — sits between
the extractor and the validator. Converts raw JDE F0101 records into
Odoo res.partner-compatible dicts. Does not validate — invalid data
is passed through so the validator can catch and report it cleanly.

Input:  Raw JDE record dict with JDE column names as keys
        (AN8, ALPH, AT1, PH1, ADD1, ADD2, CTY1, ADDS, ADDZ, COUN, TAX, PA8, UPMJ)
Output: Odoo-ready dict with res.partner field names as keys
        (name, phone, street, city, zip, customer_rank, is_company, etc.)
"""

import re
from datetime import datetime, timedelta
from utils.logger import get_logger

logger = get_logger(__name__)

# AT1 values in JDE F0101 Address Book that map to Odoo customer_rank=1
# Only 'C' (Customer) gets customer_rank=1 — vendors and employees are 0
JDE_CUSTOMER_TYPE = "C"

# PA8=0 in JDE means no parent — the record is a top-level entity
# We treat 0 as no parent rather than looking up address number 0
JDE_NO_PARENT = "0"


class CustomerTransformer:
    """
    Transforms raw JDE F0101 records into Odoo res.partner field format.
    One transformer instance handles all records in a migration run.
    """

    def transform(self, record: dict) -> dict:
        """
        Transform a single raw JDE record into an Odoo-ready dict.

        Does not raise on bad data — invalid values are passed through
        as-is so the validator can catch them and include them in the
        reconciliation report with a human-readable reason.

        Args:
            record (dict): Raw JDE record from the extractor. Keys are
                           JDE column names (AN8, ALPH, AT1, etc.)

        Returns:
            dict: Odoo res.partner compatible record. Includes a
                  _jde_an8 key for ID mapping and audit purposes.
        """
        logger.debug(f"Transforming record AN8={record.get('AN8')}")

        return {
            # Internal key — not sent to Odoo. Used to map JDE AN8
            # back to the Odoo partner ID after creation (audit trail)
            "_jde_an8": self._to_int(record.get("AN8")),

            # Core identity fields
            "name": self._clean_string(record.get("ALPH")),
            "is_company": True,  # F0101 customer records are always companies

            # customer_rank=1 means this partner appears in customer lists
            # customer_rank=0 means vendor or employee — different workflow
            "customer_rank": 1 if record.get("AT1") == JDE_CUSTOMER_TYPE else 0,
            # Raw AT1 preserved for validator Rule 4
            "_jde_at1": record.get("AT1"),

            # Contact
            "phone": self._normalize_phone(record.get("PH1")),

            # Address — passed through as-is (see docstring on unstructured data)
            "street": self._clean_string(record.get("ADD1")),
            "street2": self._clean_string(record.get("ADD2")),
            "city": self._clean_string(record.get("CTY1")),
            "zip": self._clean_string(record.get("ADDZ")),

            # state_id and country_id need reference lookups against Odoo
            # at load time — stored as raw codes here, resolved in the loader
            "state_code": self._clean_string(record.get("ADDS")),
            "country_code": self._clean_string(record.get("COUN")),

            # Financial
            "vat": self._clean_string(record.get("TAX")),

            # Parent hierarchy — stored as JDE AN8 for now.
            # Resolved to Odoo partner ID during two-pass load in Phase 2.
            "parent_an8": record.get("PA8") if record.get("PA8") != JDE_NO_PARENT else None,

            # Audit metadata — last update from JDE stored as comment
            "comment": self._build_audit_comment(record),
        }

    def transform_batch(self, records: list[dict]) -> list[dict]:
        """
        Transform a list of raw JDE records.

        Args:
            records (list[dict]): List of raw JDE records from extractor.

        Returns:
            list[dict]: List of Odoo-ready dicts, same order as input.
        """
        logger.info(f"Transforming batch of {len(records)} records")
        transformed = [self.transform(record) for record in records]
        logger.info(f"Transformation complete")
        return transformed

    # ------------------------------------------------------------------ #
    # Private helper methods                                               #
    # ------------------------------------------------------------------ #

    def _clean_string(self, value) -> str | None:
        """
        Strip leading/trailing whitespace from a string value.

        Args:
            value: Raw value from JDE record. May be None or non-string.

        Returns:
            str | None: Cleaned string, or None if value was None/empty.
        """
        if value is None:
            return None
        cleaned = str(value).strip()
        # Return None for empty strings — consistent null representation
        return cleaned if cleaned else None

    def _to_int(self, value) -> int | None:
        """
        Convert a string value to integer.

        Args:
            value: String representation of an integer (e.g. '1001').

        Returns:
            int | None: Integer value, or None if conversion fails.
        """
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _normalize_phone(self, value) -> str | None:
        """
        Normalize a JDE phone number to a consistent format.

        Strips spaces and dashes only — preserves the number structure
        so the validator can still check the format after normalization.
        Does not reformat to a canonical pattern because JDE stores both
        Philippine mobile and landline numbers in the same field.

        Args:
            value: Raw phone string from PH1 column.

        Returns:
            str | None: Normalized phone string, or None if empty.
        """
        if value is None:
            return None
        # Remove spaces and dashes — preserve + prefix and digits
        normalized = re.sub(r"[\s\-]", "", str(value))
        return normalized if normalized else None

    def _julian_to_date(self, julian_value) -> datetime | None:
        """
        Convert a JDE Julian date integer to a Python datetime object.

        JDE Julian format: (years_since_1900 * 1000) + day_of_year
        Example: 126072 → year=2026, day=72 → March 13, 2026

        Args:
            julian_value: Julian date as string or int (e.g. '126072').

        Returns:
            datetime | None: Converted date, or None if conversion fails.
        """
        if julian_value is None:
            return None
        try:
            julian_int = int(julian_value)

            # Extract year: integer division gives years since 1900
            years_since_1900 = julian_int // 1000
            actual_year = years_since_1900 + 1900

            # Extract day of year: remainder after removing year component
            day_of_year = julian_int % 1000

            # Build date: start at Jan 1 and add (day_of_year - 1) days
            # Subtract 1 because Jan 1 is already day 1, not day 0
            date = datetime(actual_year, 1, 1) + timedelta(days=day_of_year - 1)
            return date

        except (ValueError, TypeError):
            return None

    def _build_audit_comment(self, record: dict) -> str:
        """
        Build a migration audit comment from JDE metadata fields.
        Stored in Odoo partner comment field for traceability.

        Args:
            record (dict): Raw JDE record containing UPMJ and UPMT.

        Returns:
            str: Human-readable audit string for the Odoo comment field.
        """
        last_updated = self._julian_to_date(record.get("UPMJ"))
        date_str = last_updated.strftime("%Y-%m-%d") if last_updated else "unknown"
        return f"Migrated from JDE F0101 | AN8={record.get('AN8')} | JDE last updated: {date_str}"
    