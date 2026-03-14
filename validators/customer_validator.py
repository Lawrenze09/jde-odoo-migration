"""
validators/customer_validator.py

Role in pipeline: Third stage of the ETL pipeline — sits between the
transformer and the loader. Applies 8 business rules to every transformed
record. Splits records into two lists: valid (safe to load) and failed
(needs human review). Failed records include the rule that failed and a
plain English reason for the reconciliation report.

Input:  list[dict] of transformed Odoo-ready records from CustomerTransformer
Output: tuple(list[dict], list[dict]) — (valid_records, failed_records)
        failed_records include extra keys: _failed_rule, _failure_reason
"""

import re
from utils.logger import get_logger

logger = get_logger(__name__)

# Valid JDE address types in F0101
# Any value outside this set is a data quality problem
VALID_ADDRESS_TYPES = {"C", "V", "E"}

# Philippine mobile number pattern: 09XXXXXXXXX or +639XXXXXXXXX
PH_MOBILE_PATTERN = re.compile(r"^(09\d{9}|\+639\d{9})$")

# Philippine landline pattern: 02XXXXXXXX, +632XXXXXXXX, or regional
# Regional landlines: +63XX XXXXXXX (stripped of spaces by transformer)
PH_LANDLINE_PATTERN = re.compile(r"^(0\d{9,10}|\+63\d{9,11})$")


class CustomerValidator:
    """
    Validates transformed JDE customer records against 8 business rules.
    Tracks seen AN8 values to catch duplicates across the full record set.
    One validator instance must process all records in a single run —
    creating a new instance resets the duplicate tracking.
    """

    def __init__(self):
        """Initialize validator. Duplicate detection uses pre-scan, not instance state."""
        pass

    def validate_batch(self, records: list[dict]) -> tuple[list[dict], list[dict]]:
        """
        Validate a list of transformed records against all 8 business rules.

        Args:
            records (list[dict]): Transformed Odoo-ready records from
                                  CustomerTransformer.transform_batch()

        Returns:
            tuple: (valid_records, failed_records)
                   valid_records  — safe to load into Odoo
                   failed_records — each record includes _failed_rule
                                    and _failure_reason keys
        """
        valid_records = []
        failed_records = []

        logger.info(f"Validating batch of {len(records)} records")

        # Pre-scan for duplicate AN8 values before individual validation.
        # Both occurrences of a duplicate must fail — not just the second one.
        # We cannot know which duplicate is correct, so neither is safe to load.
        an8_counts: dict = {}
        for record in records:
            an8 = record.get("_jde_an8")
            if an8 is not None:
                an8_counts[an8] = an8_counts.get(an8, 0) + 1
        duplicate_an8s = {an8 for an8, count in an8_counts.items() if count > 1}
        
        if duplicate_an8s:
            logger.warning(f"Duplicate AN8 values detected: {duplicate_an8s}")

        for record in records:
            failure = self._validate_record(record, duplicate_an8s)
            if failure:
                # Attach failure metadata so the report can explain it
                failed_record = record.copy()
                failed_record["_failed_rule"] = failure["rule"]
                failed_record["_failure_reason"] = failure["reason"]
                failed_records.append(failed_record)
            else:
                valid_records.append(record)

        logger.info(
            f"Validation complete | "
            f"valid: {len(valid_records)} | "
            f"failed: {len(failed_records)}"
        )
        return valid_records, failed_records

    def _validate_record(self, record: dict, duplicate_an8s: set) -> dict | None:
        """
        Apply all 8 validation rules to a single record.
        Returns on the first failure — one failure reason per record.

        Args:
            record (dict): Single transformed record from CustomerTransformer.
            duplicate_an8s (set): AN8 values that appear more than once
                                  in the batch — both occurrences must fail.

        Returns:
            dict | None: {"rule": str, "reason": str} if validation fails,
                         None if the record passes all rules.
        """

        # Rule 1 — AN8 must not be None or empty
        # AN8 is the primary key. A record without it cannot be traced
        # back to JDE after migration — audit trail is broken.
        an8 = record.get("_jde_an8")
        if an8 is None:
            return {
                "rule": "RULE_01_AN8_REQUIRED",
                "reason": "Address Number (AN8) is missing. "
                          "Cannot migrate a record with no JDE primary key."
            }

        # Rule 2 — AN8 must be unique across all records in this batch.
        # Both occurrences of a duplicate fail — we cannot know which is correct.
        # Flagged for human review — business lead decides which record is valid.
        if an8 in duplicate_an8s:
            return {
                "rule": "RULE_02_AN8_DUPLICATE",
                "reason": f"Address Number {an8} appears more than once. "
                          f"Both records are blocked. Resolve the duplicate in "
                          f"JDE before migrating either record."
            }

        # Rule 3 — Name must not be empty
        # Odoo requires res.partner.name — it is a mandatory field.
        # A nameless partner cannot be created via XML-RPC.
        if not record.get("name"):
            return {
                "rule": "RULE_03_NAME_REQUIRED",
                "reason": f"Customer name (ALPH) is missing for AN8={an8}. "
                          f"Odoo requires a name to create a partner record."
            }

        # Rule 4 — Address type must be C, V, or E
        # Raw AT1 value passed through by transformer as _jde_at1
        at1 = record.get("_jde_at1")
        if at1 not in VALID_ADDRESS_TYPES:
            return {
                "rule": "RULE_04_INVALID_ADDRESS_TYPE",
                "reason": f"AN8={an8} has address type '{at1}' which is not valid. "
                          f"Valid types are C (Customer), V (Vendor), E (Employee)."
            }
            
        # Rule 5 — Phone format must match Philippine mobile or landline
        # Skipped if phone is None — missing phone is caught by Rule 5b below
        phone = record.get("phone")
        if phone is None:
            return {
                "rule": "RULE_05_PHONE_MISSING",
                "reason": f"Phone number (PH1) is missing for AN8={an8}. "
                          f"A contact number is required for customer records."
            }
        if not (PH_MOBILE_PATTERN.match(phone) or PH_LANDLINE_PATTERN.match(phone)):
            return {
                "rule": "RULE_05_PHONE_FORMAT",
                "reason": f"Phone number '{phone}' for AN8={an8} does not match "
                          f"Philippine mobile (09XXXXXXXXX) or landline (+63XXXXXXXXX) format."
            }

        # Rule 6 — Street address must not be empty
        if not record.get("street"):
            return {
                "rule": "RULE_06_ADDRESS_REQUIRED",
                "reason": f"Street address (ADD1) is missing for AN8={an8}. "
                          f"A physical address is required for customer records."
            }

        # Rule 7 — City must not be empty
        if not record.get("city"):
            return {
                "rule": "RULE_07_CITY_REQUIRED",
                "reason": f"City (CTY1) is missing for AN8={an8}. "
                          f"City is required for address validation in Odoo."
            }

        # Rule 8 — Zip code must be numeric if present
        # Zip is optional — but if provided it must be a valid numeric code
        zip_code = record.get("zip")
        if zip_code is not None and not str(zip_code).isdigit():
            return {
                "rule": "RULE_08_ZIP_FORMAT",
                "reason": f"Zip code '{zip_code}' for AN8={an8} is not numeric. "
                          f"Philippine postal codes must contain digits only."
            }

        # All rules passed
        return None
    