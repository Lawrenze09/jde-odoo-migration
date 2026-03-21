"""
transformers/item_transformer.py

Role in pipeline: Transforms raw JDE F4101 Item Master records into
Odoo product.template field format.

Responsibilities (pure — no external calls, no validation):
    - Normalize strings: strip whitespace, uppercase STKT and UOM codes
    - Map STKT → Odoo product type + sale_ok + purchase_ok flags
    - Normalize price: string → float | None (never defaults to 0.0)
    - Convert Julian UPMJ to audit comment
    - Preserve raw JDE keys for the validator (_jde_itm, _jde_stkt)

Input:  list[dict] of raw F4101 records from ItemExtractor
Output: list[dict] of transformed records ready for ItemValidator

The transformer never enforces business rules — that is the validator's job.
Negative prices pass through as floats. Unknown STKT codes pass through raw.
The validator catches all such problems with explicit failure reasons.
"""

from datetime import date, timedelta
from utils.logger import get_logger

logger = get_logger(__name__)

# ── STKT → Odoo product type mapping ───────────────────────────────────────
# Maps JDE stocking type to Odoo product.template 'type' field value.
# Valid STKT values: S (Stocked), N (Non-stocked), O (Outside operations)
STKT_TO_ODOO_TYPE = {
    "S": "consu",   # Stocked → storable product (inventory tracked)
    "N": "consu",     # Non-stocked → consumable (no inventory tracking)
    "O": "service",   # Outside operations → service
}

# ── STKT → sale_ok / purchase_ok behavior ──────────────────────────────────
# Explicit flag control prevents silent misconfiguration.
# Outside operations (O) are not directly sellable — they represent
# subcontracted work that appears on purchase orders, not sales orders.
STKT_BEHAVIOR = {
    "S": {"sale_ok": True,  "purchase_ok": True},
    "N": {"sale_ok": True,  "purchase_ok": True},
    "O": {"sale_ok": False, "purchase_ok": True},
}


class ItemTransformer:
    """
    Transforms raw JDE F4101 records into Odoo product.template format.
    Pure transformation only — no validation, no external calls.
    """

    def transform_batch(self, records: list[dict]) -> list[dict]:
        """
        Transform a batch of raw F4101 records.

        Args:
            records (list[dict]): Raw JDE records from ItemExtractor.
                                  Keys match F4101 column names exactly.

        Returns:
            list[dict]: Transformed records ready for ItemValidator.
        """
        logger.info(f"Transforming batch of {len(records)} item records")
        transformed = [self._transform_one(r) for r in records]
        logger.info("Item transformation complete")
        return transformed

    def _transform_one(self, record: dict) -> dict:
        """
        Transform a single raw F4101 record.

        Args:
            record (dict): Raw JDE F4101 record.

        Returns:
            dict: Transformed record with Odoo field names as keys.
        """
        # ── Normalize raw values first ────────────────────────────────
        # Strip and uppercase before any mapping so validator always
        # receives clean, consistent values regardless of JDE source format.
        raw_itm  = record.get("ITM")
        raw_stkt = (record.get("STKT") or "").strip().upper()
        raw_uom1 = (record.get("UOM1") or "").strip().upper()
        raw_uom2 = (record.get("UOM2") or "").strip().upper()
        raw_dsc1 = (record.get("DSC1") or "").strip()
        raw_dsc2 = (record.get("DSC2") or "").strip()
        raw_srp1 = record.get("SRP1")
        raw_upmj = record.get("UPMJ")

        # ── ITM: normalize to integer ─────────────────────────────────
        itm = self._normalize_itm(raw_itm)

        # ── STKT: map to Odoo product type and behavior flags ─────────
        odoo_type   = STKT_TO_ODOO_TYPE.get(raw_stkt)
        behavior    = STKT_BEHAVIOR.get(raw_stkt, {"sale_ok": True, "purchase_ok": True})

        # ── Price: string → float | None ─────────────────────────────
        list_price  = self._normalize_price(raw_srp1)

        # ── Audit comment ────────────────────────────────────────────
        comment     = self._build_comment(raw_upmj, itm)

        return {
            # ── Internal pipeline keys (not written to Odoo) ──────────
            # Preserved for the validator — raw JDE values before mapping
            "_jde_itm":   itm,
            "_jde_stkt":  raw_stkt,
            "_jde_uom1":  raw_uom1,
            "_jde_uom2":  raw_uom2,

            # ── Odoo product.template fields ──────────────────────────
            "default_code": str(itm) if itm is not None else None,
            "name":         raw_dsc1 or None,
            "description":  raw_dsc2 or None,
            "type":         odoo_type,
            "sale_ok":      behavior["sale_ok"],
            "purchase_ok":  behavior["purchase_ok"],

            # UOM fields — left as JDE codes here.
            # ItemValidator resolves these to Odoo IDs using UomRegistry.
            # Loader uses the resolved IDs — transformer stays pure.
            "uom_id":    raw_uom1 or None,
            "uom_po_id": raw_uom2 or None,

            "list_price": list_price,
            "comment":    comment,
        }

    # ── Private normalization helpers ───────────────────────────────────────

    def _normalize_itm(self, value) -> int | None:
        """
        Normalize the JDE ITM (short item number) to a Python integer.

        Args:
            value: Raw ITM value from JDE — typically a string from CSV

        Returns:
            int: Parsed item number
            None: If value is empty, None, or cannot be parsed
        """
        if value is None:
            return None
        try:
            cleaned = str(value).strip()
            if not cleaned:
                return None
            return int(float(cleaned))
        except (ValueError, TypeError):
            return None

    def _normalize_price(self, value) -> float | None:
        """
        Normalize a JDE SRP1 price string to a Python float.

        Returns None for empty or missing values — never defaults to 0.0
        because 0.0 means "free product" in Odoo, which silently corrupts
        pricing data when the JDE field was simply not populated.

        Negative values are returned as-is (e.g. -100.0) — the validator
        enforces the >= 0 business rule, not the transformer. This keeps
        transformation and validation cleanly separated.

        Args:
            value: Raw SRP1 string e.g. "450.00", "", "-100.00", "abc"

        Returns:
            float: Parsed price if value is a valid numeric string
            None:  If value is empty, None, or cannot be parsed as float
        """
        if value is None:
            return None
        try:
            cleaned = str(value).strip()
            if not cleaned:
                return None
            return float(cleaned)
        except (ValueError, TypeError):
            return None

    def _build_comment(self, upmj: str | None, itm: int | None) -> str:
        """
        Build an audit comment from the JDE last-update Julian date.
        Mirrors the pattern used in CustomerTransformer for consistency.

        Args:
            upmj (str | None): JDE Julian date of last update
            itm  (int | None): JDE item number for traceability

        Returns:
            str: Audit comment e.g.
                 "Migrated from JDE F4101 | ITM=2002 | Last updated: 2026-03-13"
        """
        try:
            upmj_int  = int(str(upmj).strip())
            year      = (upmj_int // 1000) + 1900
            day_of_yr = upmj_int % 1000
            last_updated = date(year, 1, 1) + timedelta(days=day_of_yr - 1)
            date_str  = last_updated.strftime("%Y-%m-%d")
        except (ValueError, TypeError, AttributeError):
            date_str = "unknown"

        return f"Migrated from JDE F4101 | ITM={itm} | Last updated: {date_str}"
    