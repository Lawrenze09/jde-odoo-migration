"""
sync/conflict_resolver.py

Role in pipeline: Determines what to do when a JDE record already exists
in Odoo. Called by the sync engine before every load decision.

Three strategies — configured via settings or CLI:

    JDE_WINS  — Odoo record is updated with JDE data.
                Use when JDE is the authoritative source of truth.
                Default during initial migration and parallel running.

    ODOO_WINS — Odoo record is kept as-is. JDE update is ignored.
                Use when Odoo has been manually corrected and those
                corrections must be preserved over JDE data.

    FLAG      — Conflict is logged and added to the report for review.
                Neither system's data is written. A human decides.
                Use when neither system is trusted over the other.

Change detection:
    Before applying any strategy, a MD5 hash of the relevant JDE fields
    is compared against a stored hash of the last-loaded data. If hashes
    match, the record has not changed — no action taken regardless of strategy.
    This prevents unnecessary API calls and protects manual Odoo corrections
    that haven't been synced back to JDE.
"""

import hashlib
import json
from enum import Enum
from dataclasses import dataclass
from utils.logger import get_logger

logger = get_logger(__name__)


class ConflictStrategy(Enum):
    """
    Resolution strategy when a JDE record already exists in Odoo.
    Enum prevents typo bugs — ConflictStrategy.JDE_WINS is safe,
    the string "JDE_wins" would silently fail comparisons.
    """
    JDE_WINS  = "JDE_WINS"   # Update Odoo with JDE data
    ODOO_WINS = "ODOO_WINS"  # Keep Odoo data, ignore JDE update
    FLAG      = "FLAG"       # Log conflict, skip, flag for review


class ConflictAction(Enum):
    """
    The action the loader should take after conflict resolution.
    Returned by ConflictResolver.resolve() for every record.
    """
    UPDATE = "UPDATE"   # Write JDE data to Odoo
    SKIP   = "SKIP"     # Do not write — keep Odoo as-is
    FLAG   = "FLAG"     # Do not write — add to report for human review
    NONE   = "NONE"     # No conflict — record does not exist in Odoo yet


@dataclass
class ConflictResult:
    """
    Result of a conflict resolution decision for one record.

    Attributes:
        action:   What the loader should do with this record
        reason:   Human-readable explanation for the report
        an8:      JDE Address Number for traceability
        strategy: Which strategy was applied
    """
    action:   ConflictAction
    reason:   str
    an8:      int
    strategy: ConflictStrategy | None = None


# Fields included in the change detection hash.
# Only fields that are actually migrated to Odoo — internal pipeline
# keys (_jde_an8, _jde_at1) and audit fields (comment) are excluded
# because they change on every run and would always trigger updates.
HASHABLE_FIELDS = [
    "name", "phone", "street", "street2", "city",
    "zip", "state_code", "country_code", "vat",
    "customer_rank", "is_company", "parent_an8",
]


def compute_record_hash(record: dict) -> str:
    """
    Compute a stable MD5 hash of the business fields in a transformed record.

    Used for change detection — if the hash of the current JDE record
    matches the hash stored from the last load, the record has not changed
    and no update is needed regardless of conflict strategy.

    Sorting keys before hashing ensures the hash is stable regardless of
    dict insertion order — Python dicts preserve insertion order but we
    want the hash to be independent of that.

    Args:
        record (dict): Transformed record from CustomerTransformer.

    Returns:
        str: Hex MD5 hash string of the record's business fields.
    """
    # Extract only the hashable fields in sorted key order
    hashable = {
        field: str(record.get(field, ""))
        for field in sorted(HASHABLE_FIELDS)
    }
    # Serialize to JSON with sorted keys for stability
    serialized = json.dumps(hashable, sort_keys=True)
    return hashlib.md5(serialized.encode("utf-8")).hexdigest()


class ConflictResolver:
    """
    Resolves conflicts between JDE source records and existing Odoo records.
    Applies change detection before strategy — no unnecessary updates.
    """

    def __init__(self, strategy: ConflictStrategy = ConflictStrategy.JDE_WINS):
        """
        Initialize the resolver with a conflict strategy.

        Args:
            strategy (ConflictStrategy): Resolution strategy to apply.
                                         Defaults to JDE_WINS — JDE is
                                         the source of truth during migration.
        """
        self.strategy = strategy
        logger.info(f"ConflictResolver initialized | strategy: {strategy.value}")

    def resolve(
        self,
        record: dict,
        existing_odoo_id: int | None,
        last_known_hash: str | None = None,
    ) -> ConflictResult:
        """
        Determine what action to take for a single record.

        Decision flow:
            1. If no existing Odoo record → no conflict → action: NONE
            2. Compute hash of current JDE data
            3. If hash matches last known hash → no change → action: SKIP
            4. Apply conflict strategy to determine action

        Args:
            record           (dict): Transformed record from CustomerTransformer.
            existing_odoo_id (int | None): Odoo partner ID if record exists,
                                           None if it does not.
            last_known_hash  (str | None): MD5 hash stored from last successful
                                           load of this record. None if never loaded.

        Returns:
            ConflictResult: Action to take and reason for the report.
        """
        an8 = record.get("_jde_an8")

        # ── No conflict: record does not exist in Odoo yet ──────────────
        if existing_odoo_id is None:
            return ConflictResult(
                action=ConflictAction.NONE,
                reason="No existing Odoo record — safe to create.",
                an8=an8,
            )

        # ── Change detection: has the record actually changed? ──────────
        current_hash = compute_record_hash(record)

        if last_known_hash and current_hash == last_known_hash:
            # Hash matches — nothing changed in JDE since last sync.
            # Skip regardless of strategy — no unnecessary API calls.
            logger.debug(
                f"No change detected | AN8={an8} | "
                f"hash={current_hash[:8]}... | skipping"
            )
            return ConflictResult(
                action=ConflictAction.SKIP,
                reason="No change detected — JDE data matches last loaded state.",
                an8=an8,
                strategy=self.strategy,
            )

        # ── Conflict exists: record changed in JDE, exists in Odoo ──────
        logger.info(
            f"Conflict detected | AN8={an8} | "
            f"Odoo ID={existing_odoo_id} | "
            f"strategy={self.strategy.value}"
        )

        if self.strategy == ConflictStrategy.JDE_WINS:
            return ConflictResult(
                action=ConflictAction.UPDATE,
                reason=(
                    f"JDE_WINS strategy — Odoo record (ID={existing_odoo_id}) "
                    f"will be updated with current JDE data."
                ),
                an8=an8,
                strategy=self.strategy,
            )

        elif self.strategy == ConflictStrategy.ODOO_WINS:
            return ConflictResult(
                action=ConflictAction.SKIP,
                reason=(
                    f"ODOO_WINS strategy — JDE update ignored. "
                    f"Odoo record (ID={existing_odoo_id}) preserved as-is."
                ),
                an8=an8,
                strategy=self.strategy,
            )

        elif self.strategy == ConflictStrategy.FLAG:
            return ConflictResult(
                action=ConflictAction.FLAG,
                reason=(
                    f"FLAG strategy — conflict flagged for human review. "
                    f"AN8={an8} changed in JDE but Odoo record "
                    f"(ID={existing_odoo_id}) was not updated. "
                    f"Review and resolve manually."
                ),
                an8=an8,
                strategy=self.strategy,
            )

        # Should never reach here — all enum values handled above
        raise ValueError(f"Unknown conflict strategy: {self.strategy}")
    