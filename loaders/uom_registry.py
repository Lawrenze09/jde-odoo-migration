"""
loaders/uom_registry.py

Role in pipeline: Shared UOM resolution registry for Phase 3 item migration.
Preloads unit-of-measure data from Odoo and the JDE→Odoo name mapping from
a CSV config file, then builds a frozen resolved lookup used by both the
validator and the item loader.

Fail-fast design — crashes at startup if:
    - The mapping CSV is missing or malformed
    - Any mapped Odoo UOM name does not exist in the live Odoo instance

This prevents partial runs and ensures the validator and loader always
see the same consistent state throughout a pipeline run.

NOTE — Odoo saas-19.2 compatibility:
    In Odoo saas-19.2+, the 'category_id' field was removed from the
    uom.uom model's XML-RPC API. Category is therefore defined in the
    mapping CSV (config/uom_mapping.csv) rather than fetched from Odoo.
    This makes category validation fully deterministic and Odoo-version
    independent.

Usage:
    registry = UomRegistry(models, uid, password, db)
    uom      = registry.resolve("EA")   # → UomRecord(id=1, name="Units", ...)
    uom_id   = registry.resolve("EA").id
    category = registry.resolve("EA").category

Registry is frozen after __init__ — never refreshed mid-run.
Validator and loader share the same instance for guaranteed consistency.
"""

import csv
import os
from dataclasses import dataclass
from utils.logger import get_logger

logger = get_logger(__name__)

# Default path to the JDE→Odoo UOM name mapping config file
DEFAULT_MAPPING_PATH = "config/uom_mapping.csv"

# UOM categories considered semantically compatible with service/outside
# operation items (STKT='O'). Used in Rule 09 semantic validation.
# "Time" covers HR (Hours) — the most common service UOM.
SERVICE_COMPATIBLE_CATEGORIES = {"Unit", "Time", "Working Time", "Unit of Time"}


@dataclass(frozen=True)
class UomRecord:
    """
    Resolved UOM data — held in the registry after startup.
    Frozen (immutable) after creation — registry never mutates these.

    Note: category_id is set to 0 for all records in Odoo saas-19.2
    because category_id is not available via XML-RPC in that version.
    Category is sourced from config/uom_mapping.csv instead.

    Attributes:
        id:          Odoo uom.uom record ID — used in product.template payload
        name:        Odoo display name e.g. 'Units', 'kg'
        category_id: Always 0 in saas-19.2 — category not available via XML-RPC
        category:    Category name from uom_mapping.csv e.g. 'Unit', 'Weight'
    """
    id:          int
    name:        str
    category_id: int
    category:    str


class UomRegistry:
    """
    Frozen UOM resolution registry — built once at startup, shared by
    ItemValidator and ItemLoader throughout the pipeline run.

    Three-phase initialization:
        1. Load Odoo UOMs via XML-RPC (id + name only — no category in saas-19.2)
        2. Load JDE→Odoo name + category mapping from CSV config
        3. Cross-reference and build resolved lookup — fails fast on mismatch

    After __init__ completes, the registry is fully resolved and frozen.
    No further Odoo calls are made for UOM resolution.
    """

    def __init__(
        self,
        models,
        uid: int,
        password: str,
        db: str,
        mapping_path: str = DEFAULT_MAPPING_PATH,
    ):
        """
        Build the UOM registry from Odoo data and the mapping CSV.
        Fails immediately if the mapping is invalid or incomplete.

        Args:
            models:       Odoo XML-RPC models proxy
            uid:          Authenticated Odoo user ID
            password:     Odoo password for XML-RPC calls
            db:           Odoo database name
            mapping_path: Path to JDE→Odoo UOM name mapping CSV

        Raises:
            FileNotFoundError: If the mapping CSV does not exist
            ValueError:        If any mapped Odoo UOM name is not found
                               in the live Odoo instance — fail fast
        """
        logger.info(f"UomRegistry initializing | mapping: {mapping_path}")

        # Phase 1: Load UOM names and IDs from Odoo — one XML-RPC call.
        # NOTE: category_id is NOT fetched — removed from uom.uom in saas-19.2.
        # Category is sourced from the mapping CSV in Phase 2.
        self._uom_by_name = self._load_odoo_uoms(models, uid, password, db)
        logger.info(f"Loaded {len(self._uom_by_name)} UOMs from Odoo")

        # Phase 2: Load JDE→Odoo name + category mapping from CSV.
        # Category column is required — this is the source of truth for
        # category validation since Odoo saas-19.2 removed category_id.
        self._mapping = self._load_mapping(mapping_path)
        logger.info(f"Loaded {len(self._mapping)} UOM mappings from config")

        # Phase 3: Cross-reference and build resolved lookup.
        # Raises ValueError immediately if any Odoo name is not found.
        # Injects category from CSV into each UomRecord.
        self._resolved = self._build_resolved_map()
        logger.info(
            f"UomRegistry ready | "
            f"{len(self._resolved)} JDE codes resolved | "
            f"frozen for this run"
        )

    def resolve(self, jde_code: str) -> UomRecord:
        """
        Resolve a JDE UOM code to a UomRecord.

        Args:
            jde_code (str): JDE UOM code e.g. 'EA', 'KG', 'L'

        Returns:
            UomRecord: Resolved Odoo UOM with id, name, and category.

        Raises:
            KeyError: If the JDE code is not in the resolved map.
                      Validator catches this and fails the record with Rule 05.
                      Loader should never see this — validator runs first.
        """
        if jde_code not in self._resolved:
            raise KeyError(
                f"JDE UOM code '{jde_code}' not in UOM registry. "
                f"Add it to config/uom_mapping.csv to resolve. "
                f"Known codes: {sorted(self._resolved.keys())}"
            )
        return self._resolved[jde_code]

    def is_resolvable(self, jde_code: str) -> bool:
        """
        Check whether a JDE UOM code can be resolved without raising.
        Used by the validator for clean boolean checks.

        Args:
            jde_code (str): JDE UOM code to check

        Returns:
            bool: True if the code resolves, False otherwise
        """
        return jde_code in self._resolved

    def known_codes(self) -> list[str]:
        """
        Return all JDE UOM codes known to this registry.
        Used for error messages and debugging.

        Returns:
            list[str]: Sorted list of resolvable JDE UOM codes
        """
        return sorted(self._resolved.keys())

    # ── Private initialization methods ──────────────────────────────────────

    def _load_odoo_uoms(
        self,
        models,
        uid: int,
        password: str,
        db: str,
    ) -> dict[str, UomRecord]:
        """
        Load UOM names and IDs from Odoo via XML-RPC.
        Builds a dict keyed by Odoo display name for fast lookup.

        Only fetches 'id' and 'name' — 'category_id' was removed from
        the uom.uom XML-RPC API in Odoo saas-19.2. Category is sourced
        from the mapping CSV and injected in _build_resolved_map().

        Args:
            models:   Odoo XML-RPC models proxy
            uid:      Authenticated Odoo user ID
            password: Odoo password
            db:       Odoo database name

        Returns:
            dict[str, UomRecord]: Odoo UOM name → partial UomRecord
                                  (category populated later from CSV)
        """
        raw = models.execute_kw(
            db, uid, password,
            "uom.uom",
            "search_read",
            [[]],
            {"fields": ["id", "name"]},
        )

        result = {}
        for record in raw:
            name = record["name"]
            # category_id=0 and category="" are placeholders —
            # overwritten with CSV values in _build_resolved_map()
            uom = UomRecord(
                id=record["id"],
                name=name,
                category_id=0,
                category="",
            )
            if name in result:
                logger.warning(
                    f"Duplicate Odoo UOM name '{name}' — "
                    f"keeping ID={result[name].id}, ignoring ID={uom.id}"
                )
            else:
                result[name] = uom

        return result

    def _load_mapping(self, mapping_path: str) -> dict[str, dict]:
        """
        Load JDE UOM code → Odoo name + category mapping from CSV config.

        Expected CSV format (with header):
            jde_code,odoo_name,category
            EA,Units,Unit
            KG,kg,Weight
            HR,Hours,Time

        The 'category' column is required because Odoo saas-19.2 removed
        category_id from the XML-RPC API. Category is deployment-specific
        and must be defined explicitly in the mapping file.

        Args:
            mapping_path (str): Path to the mapping CSV file

        Returns:
            dict[str, dict]: JDE code → {"odoo_name": str, "category": str}

        Raises:
            FileNotFoundError: If the CSV file does not exist
            ValueError:        If the CSV is missing required columns
        """
        if not os.path.exists(mapping_path):
            raise FileNotFoundError(
                f"UOM mapping file not found: {mapping_path}. "
                f"Create config/uom_mapping.csv with columns: "
                f"jde_code, odoo_name, category"
            )

        mapping = {}
        with open(mapping_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for required_col in ["jde_code", "odoo_name", "category"]:
                if required_col not in (reader.fieldnames or []):
                    raise ValueError(
                        f"UOM mapping CSV missing '{required_col}' column. "
                        f"Found columns: {reader.fieldnames}"
                    )

            for row in reader:
                jde_code  = row["jde_code"].strip().upper()
                odoo_name = row["odoo_name"].strip()
                category  = row["category"].strip()
                if jde_code and odoo_name:
                    mapping[jde_code] = {
                        "odoo_name": odoo_name,
                        "category":  category,
                    }

        return mapping

    def _build_resolved_map(self) -> dict[str, UomRecord]:
        """
        Cross-reference JDE→Odoo name mapping against live Odoo UOMs.
        Builds the final resolved lookup: JDE code → UomRecord.
        Injects category from CSV into each UomRecord.

        Fails fast with a clear error listing ALL unresolvable mappings —
        forces the operator to fix the config before any data is processed.

        Returns:
            dict[str, UomRecord]: JDE UOM code → fully resolved UomRecord

        Raises:
            ValueError: If any mapping references an Odoo UOM name
                        that does not exist in the live instance
        """
        resolved = {}
        errors   = []

        for jde_code, entry in self._mapping.items():
            odoo_name = entry["odoo_name"]
            category  = entry["category"]

            if odoo_name not in self._uom_by_name:
                errors.append(
                    f"  JDE '{jde_code}' → Odoo '{odoo_name}': "
                    f"not found in Odoo uom.uom. "
                    f"Check uom.uom records or update uom_mapping.csv."
                )
            else:
                base = self._uom_by_name[odoo_name]
                # Build new frozen UomRecord injecting category from CSV
                resolved[jde_code] = UomRecord(
                    id=base.id,
                    name=base.name,
                    category_id=0,      # not available in saas-19.2
                    category=category,
                )
                logger.debug(
                    f"Resolved | JDE '{jde_code}' → "
                    f"Odoo '{odoo_name}' "
                    f"(ID={resolved[jde_code].id}, "
                    f"category={category})"
                )

        if errors:
            error_list = "\n".join(errors)
            raise ValueError(
                f"UOM registry startup failed — "
                f"{len(errors)} unresolvable mapping(s):\n{error_list}"
            )

        return resolved
    