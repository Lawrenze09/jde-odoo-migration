# JDE to Odoo Migration Toolkit

Python ETL pipeline for migrating Oracle JDE master data to Odoo ERP.

---

## Data Disclaimer

**All data in `mock_data/` is entirely fictional.**

The records in this repository were synthetically generated for development
and testing purposes only. Any resemblance to real businesses, persons,
or organizations is coincidental.

This repository does not contain, reference, or reproduce any proprietary,
confidential, or client-owned data. No data from any company, client, or
organization was used in the creation of this project.

This project was built as a personal portfolio project to demonstrate
ETL engineering skills. It is not affiliated with, endorsed by, or
connected to any employer or client.

---

## Status

Phase 1 complete — Customer master data (F0101) migration fully operational.
Phase 2 complete — Incremental sync with UPMJ+UPMT watermark tracking.
Phase 3 in progress — Item master data (F4101) migration.

## What it does

Extracts customer records from JDE F0101 → transforms and validates
against 8 business rules → loads to Odoo res.partner via XML-RPC →
generates Excel reconciliation report.

## Usage

```bash
# Dry run — preview without writing to Odoo
python main.py --table customers --dry-run --report

# Live run — write to Odoo
python main.py --table customers --source mock --report

# Limit records for testing
python main.py --table customers --dry-run --limit 10
```

## Pipeline stages

1. Extract — reads from mock CSV or Oracle JDE database
2. Transform — converts JDE formats (Julian dates, phone normalization)
3. Validate — 8 business rules, catches data quality problems
4. Load — atomic batch to Odoo with idempotent protection
5. Report — 3-sheet Excel report with success/failure breakdown

## Data quality rules

- AN8 must be present and unique
- Customer name must not be empty
- Address type must be C, V, or E
- Phone must match Philippine mobile or landline format
- Street address and city must be present
- Zip code must be numeric if provided

## UOM Mapping

The JDE→Odoo unit of measure mapping is configured in `config/uom_mapping.csv`.
This file must be customized for each Odoo deployment.

### Why this matters

Every Odoo instance has different UOM names depending on version, locale,
and installed modules. The exact names were verified against this project's
Odoo instance using:

    models.execute_kw(db, uid, password, "uom.uom", "search_read",
                        [[]], {"fields": ["id", "name"]})

### Current mapping (verified against Odoo saas-19.2 free trial)

| JDE Code | Odoo Name | Category | Notes                                          |
| -------- | --------- | -------- | ---------------------------------------------- |
| EA       | Units     | Unit     | Each/Unit                                      |
| CS       | Units     | Unit     | Case — flattened to base unit (simplification) |
| KG       | kg        | Weight   | Kilogram                                       |
| L        | L         | Volume   | Liter                                          |
| HR       | Hours     | Time     | Hour                                           |

### For a different Odoo deployment

1. Run the UOM discovery script to get exact names from your instance
2. Update `config/uom_mapping.csv` with the correct `odoo_name` values
3. The `category` column is deployment-independent — keep your own category grouping

**Note:** `CS` (Case) is intentionally mapped to `Units` as a simplification.
In a full implementation, define a custom "Case" UOM in Odoo with a conversion
ratio to Units.

```

## Project structure

See individual module docstrings for full documentation.
```
