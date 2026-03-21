"""
validators/item_validator.py

Role in pipeline: Enforces 9 business rules on transformed F4101 records.
Receives output from ItemTransformer and produces two lists:
    valid_records   — passed all rules, safe to send to ItemLoader
    failed_records  — failed at least one rule, logged with plain English reason

Pure validator — no external calls during record processing.
Depends on a UomRegistry snapshot injected at construction time.
The registry is frozen at startup — validator sees the same UOM state
for every record in the batch, regardless of how long validation takes.

Validation rules:
    Rule 01 — ITM must be present, numeric, and positive
    Rule 02 — ITM must be unique within the batch (pre-scan, both fail)
    Rule 03 — DSC1 (name) must not be empty
    Rule 04 — STKT must be in {S, N, O}
    Rule 05 — UOM1 must be present and resolvable in the UOM registry
    Rule 06 — UOM2 must be resolvable if present
    Rule 07 — UOM1 and UOM2 must belong to the same category if both present
    Rule 08 — SRP1 (list_price) must be non-negative if present
    Rule 09 — STKT and UOM1 category must be semantically consistent
               (service items must not use weight or volume UOMs)
"""

from loaders.uom_registry import UomRegistry, SERVICE_COMPATIBLE_CATEGORIES
from utils.logger import get_logger

logger = get_logger(__name__)

# Valid JDE stocking type codes
VALID_STKT_CODES = {"S", "N", "O"}


class ItemValidator:
    """
    Validates transformed F4101 item records against 9 business rules.

    Requires a UomRegistry instance at construction time — the registry
    is frozen at startup and shared with ItemLoader for consistency.
    No Odoo calls are made during validation.
    """

    def __init__(self, uom_registry: UomRegistry):
        """
        Initialize the validator with a frozen UOM registry.

        Args:
            uom_registry (UomRegistry): Frozen UOM resolution registry.
                                        Built once at pipeline startup and
                                        shared by validator and loader.
        """
        self.uom_registry = uom_registry
        logger.info("ItemValidator initialized")

    def validate_batch(
        self,
        records: list[dict],
    ) -> tuple[list[dict], list[dict]]:
        """
        Validate a batch of transformed F4101 records.

        Pre-scans for duplicate ITM values and unknown UOM codes before
        processing individual records — provides batch-level warnings
        that tell the operator exactly what to fix before re-running.

        Args:
            records (list[dict]): Transformed records from ItemTransformer.

        Returns:
            tuple: (valid_records, failed_records)
                   valid_records  — passed all 9 rules
                   failed_records — failed at least one rule,
                                    with _failed_rule and _failure_reason keys
        """
        logger.info(f"Validating batch of {len(records)} item records")

        # ── Pre-scan 1: duplicate ITM detection ──────────────────────
        # Both occurrences of a duplicate fail — same design as AN8 in
        # CustomerValidator. Prevents partial loads of duplicate items.
        itm_counts = {}
        for record in records:
            itm = record.get("_jde_itm")
            if itm is not None:
                itm_counts[itm] = itm_counts.get(itm, 0) + 1

        duplicate_itms = {itm for itm, count in itm_counts.items() if count > 1}
        if duplicate_itms:
            logger.warning(f"Duplicate ITM values detected: {duplicate_itms}")

        # ── Pre-scan 2: unknown UOM codes detection ───────────────────
        # Collect all unique UOM codes in the batch and check them all
        # against the registry upfront. Logs a single actionable warning
        # listing exactly which codes need to be added to uom_mapping.csv.
        all_uom_codes = set()
        for record in records:
            uom1 = record.get("_jde_uom1")
            uom2 = record.get("_jde_uom2")
            if uom1:
                all_uom_codes.add(uom1)
            if uom2:
                all_uom_codes.add(uom2)

        unknown_uom_codes = {
            code for code in all_uom_codes
            if not self.uom_registry.is_resolvable(code)
        }
        if unknown_uom_codes:
            logger.warning(
                f"Unknown UOM codes in batch: {unknown_uom_codes} — "
                f"add to config/uom_mapping.csv to resolve. "
                f"Known codes: {self.uom_registry.known_codes()}"
            )

        # ── Per-record validation ─────────────────────────────────────
        valid_records  = []
        failed_records = []

        for record in records:
            failed_rule, reason = self._validate_one(record, duplicate_itms)

            if failed_rule:
                failed_records.append({
                    **record,
                    "_failed_rule":    failed_rule,
                    "_failure_reason": reason,
                })
            else:
                valid_records.append(record)

        logger.info(
            f"Item validation complete | "
            f"valid: {len(valid_records)} | "
            f"failed: {len(failed_records)}"
        )
        return valid_records, failed_records

    def _validate_one(
        self,
        record: dict,
        duplicate_itms: set,
    ) -> tuple[str | None, str | None]:
        """
        Validate a single transformed record against all 9 rules.
        Returns on the first failed rule — records report one failure reason.

        Args:
            record:         Transformed record from ItemTransformer.
            duplicate_itms: Set of ITM values that appear more than once
                            in the batch — pre-computed for Rule 02.

        Returns:
            tuple: (failed_rule, reason) if any rule fails
                   (None, None) if all rules pass
        """
        itm       = record.get("_jde_itm")
        stkt      = record.get("_jde_stkt")
        uom1      = record.get("_jde_uom1")
        uom2      = record.get("_jde_uom2")
        name      = record.get("name")
        price     = record.get("list_price")

        # ── Rule 01 — ITM present, numeric, and positive ──────────────
        if itm is None:
            return (
                "Rule01_ITMRequired",
                "Item number (ITM) is missing or could not be parsed as a number."
            )
        if not isinstance(itm, int) or itm <= 0:
            return (
                "Rule01_ITMRequired",
                f"Item number (ITM) must be a positive integer. Got: '{itm}'."
            )

        # ── Rule 02 — ITM unique in batch ─────────────────────────────
        if itm in duplicate_itms:
            return (
                "Rule02_ITMDuplicate",
                f"Item number ITM={itm} appears more than once in this batch. "
                f"Both occurrences are rejected — remove the duplicate."
            )

        # ── Rule 03 — Name required ───────────────────────────────────
        if not name or not str(name).strip():
            return (
                "Rule03_NameRequired",
                f"Item description (DSC1) is empty for ITM={itm}. "
                f"Every item must have a name."
            )

        # ── Rule 04 — STKT must be S, N, or O ────────────────────────
        if stkt not in VALID_STKT_CODES:
            return (
                "Rule04_InvalidSTKT",
                f"Stocking type STKT='{stkt}' is not valid for ITM={itm}. "
                f"Must be one of: S (Stocked), N (Non-stocked), O (Outside operations)."
            )

        # ── Rule 05 — UOM1 required and resolvable ────────────────────
        if not uom1:
            return (
                "Rule05_UOM1Required",
                f"Primary unit of measure (UOM1) is missing for ITM={itm}. "
                f"UOM1 is required — every item must have a sales unit."
            )
        if not self.uom_registry.is_resolvable(uom1):
            return (
                "Rule05_UOM1Required",
                f"UOM1='{uom1}' for ITM={itm} is not in the UOM mapping. "
                f"Add '{uom1}' to config/uom_mapping.csv to resolve."
            )

        # ── Rule 06 — UOM2 resolvable if present ─────────────────────
        if uom2 and not self.uom_registry.is_resolvable(uom2):
            return (
                "Rule06_UOM2Invalid",
                f"UOM2='{uom2}' for ITM={itm} is not in the UOM mapping. "
                f"Add '{uom2}' to config/uom_mapping.csv or leave blank."
            )

        # ── Rule 07 — UOM1 and UOM2 same category ────────────────────
        if uom2 and self.uom_registry.is_resolvable(uom2):
            uom1_record = self.uom_registry.resolve(uom1)
            uom2_record = self.uom_registry.resolve(uom2)
            if uom1_record.category != uom2_record.category:
                return (
                    "Rule07_UOMCategoryMismatch",
                    f"UOM category mismatch for ITM={itm}: "
                    f"UOM1='{uom1}' ({uom1_record.category}) and "
                    f"UOM2='{uom2}' ({uom2_record.category}) must be in the same category."
                )

        # ── Rule 08 — Price non-negative if present ───────────────────
        if price is not None and price < 0:
            return (
                "Rule08_NegativePrice",
                f"List price for ITM={itm} is {price}. "
                f"Price must be zero or positive if provided."
            )

        # ── Rule 09 — STKT semantically consistent with UOM1 category ─
        uom1_record = self.uom_registry.resolve(uom1)
        if stkt == "O" and uom1_record.category not in SERVICE_COMPATIBLE_CATEGORIES:
            return (
                "Rule09_STKTUOMIncompatible",
                f"Service item (STKT='O') for ITM={itm} has an incompatible "
                f"UOM1='{uom1}' in category '{uom1_record.category}'. "
                f"Service items must use a unit-based UOM (Unit, Time), "
                f"not '{uom1_record.category}'."
            )

        return None, None
    