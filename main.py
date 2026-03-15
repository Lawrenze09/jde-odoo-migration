"""
main.py

Role in pipeline: CLI entry point for the JDE-to-Odoo migration toolkit.
Accepts command-line flags to control pipeline behavior — which table to
migrate, which source to use, whether to dry run, and whether to generate
a report. Orchestrates all pipeline stages in sequence.

Usage examples:
    python main.py --table customers --dry-run --report
    python main.py --table customers --source oracle --limit 100 --report
    python main.py --table customers --source mock --report

Flags:
    --table     Which table to migrate. Currently supports: customers
    --source    Data source: mock (default) or oracle
    --dry-run   Preview mode — no data written to Odoo
    --limit     Process only the first N records
    --report    Generate Excel reconciliation report after run
"""

import argparse
import sys
from config.settings import get_settings
from utils.logger import get_logger

logger = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """
    Build and return the CLI argument parser.

    Returns:
        argparse.ArgumentParser: Configured parser with all migration flags.
    """
    parser = argparse.ArgumentParser(
        prog="main.py",
        description="JDE to Odoo ERP Migration Toolkit",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --table customers --dry-run --report
  python main.py --table customers --source oracle --limit 100
  python main.py --table customers --source mock --report
        """,
    )

    parser.add_argument(
        "--table",
        required=True,
        choices=["customers"],
        help="Which JDE table to migrate. Currently supports: customers",
    )

    parser.add_argument(
        "--source",
        default="mock",
        choices=["mock", "oracle"],
        help="Data source to use. 'mock' reads from CSV, 'oracle' connects to JDE (default: mock)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        # store_true means the flag is False by default.
        # When --dry-run is passed, it becomes True.
        # No value needed — presence of the flag is the signal.
        help="Preview mode — transform and validate but do not write to Odoo",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the first N records. Useful for testing pipeline with real data.",
    )

    parser.add_argument(
        "--report",
        action="store_true",
        help="Generate an Excel reconciliation report after the run",
    )

    return parser


def run_customer_migration(args, settings) -> int:
    """
    Execute the full customer migration pipeline.
    Extract → Transform → Validate → Load (or dry run) → Report

    Args:
        args:     Parsed argparse namespace with CLI flags.
        settings: Settings instance from get_settings().

    Returns:
        int: Exit code. 0 = success, 1 = failure.
    """
    from extractors.mock_extractor import MockExtractor
    from transformers.customer_transformer import CustomerTransformer
    from validators.customer_validator import CustomerValidator
    from loaders.csv_loader import CsvLoader

    logger.info("=" * 60)
    logger.info("JDE to Odoo Migration Toolkit")
    logger.info(f"Table:    customers (F0101)")
    logger.info(f"Source:   {args.source}")
    logger.info(f"Dry run:  {args.dry_run}")
    logger.info(f"Limit:    {args.limit or 'none'}")
    logger.info(f"Report:   {args.report}")
    logger.info("=" * 60)

    try:
        # ── Stage 1: Extract ──────────────────────────────────────────
        logger.info("STAGE 1 — Extract")

        if args.source == "mock":
            extractor = MockExtractor()
        else:
            # Oracle extractor — built in Phase 4
            logger.error("Oracle source not yet implemented. Use --source mock.")
            return 1

        records = extractor.extract()

        # Apply --limit if specified
        # Useful for testing: run first 10 records before committing to full set
        if args.limit:
            records = records[:args.limit]
            logger.info(f"Limit applied — processing {len(records)} records")

        # ── Stage 2: Transform ────────────────────────────────────────
        logger.info("STAGE 2 — Transform")
        transformer = CustomerTransformer()
        transformed = transformer.transform_batch(records)

        # ── Stage 3: Validate ─────────────────────────────────────────
        logger.info("STAGE 3 — Validate")
        validator = CustomerValidator()
        valid_records, failed_records = validator.validate_batch(transformed)

        # ── Stage 4: Load ─────────────────────────────────────────────
        logger.info("STAGE 4 — Load")

        if args.dry_run or settings.dry_run:
            # Dry run — write preview CSV instead of calling Odoo
            logger.info("DRY RUN mode — writing preview CSV, skipping Odoo")
            loader = CsvLoader()
            valid_path = loader.load(valid_records)
            failed_path = loader.load_failed(failed_records)
            if valid_path:
                logger.info(f"Dry run preview: {valid_path}")
            if failed_path:
                logger.info(f"Failed records:  {failed_path}")
        else:
            # Live run — Odoo loader (Step 16)
            logger.warning("Live Odoo load not yet implemented. Use --dry-run.")
            return 1

        # ── Stage 5: Report ───────────────────────────────────────────
        if args.report:
            logger.info("STAGE 5 — Report")
            from reports.migration_report import MigrationReport
            report = MigrationReport()
            report_path = report.generate(
                valid_records=valid_records,
                failed_records=failed_records,
                dry_run=args.dry_run or settings.dry_run,
                source=args.source,
            )
            logger.info(f"Report generated: {report_path}")

        # ── Summary ───────────────────────────────────────────────────
        logger.info("=" * 60)
        logger.info("MIGRATION COMPLETE")
        logger.info(f"Total extracted: {len(records)}")
        logger.info(f"Valid records:   {len(valid_records)}")
        logger.info(f"Failed records:  {len(failed_records)}")
        logger.info(
            f"Success rate:    "
            f"{len(valid_records) / len(records) * 100:.1f}%"
            if records else "N/A"
        )
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return 1


def main():
    """
    Parse CLI arguments and dispatch to the correct migration pipeline.
    Exit code reflects pipeline success or failure — useful for automation.
    """
    parser = build_parser()
    args = parser.parse_args()
    settings = get_settings()

    # Override dry_run: CLI flag takes precedence over .env setting.
    # If --dry-run is passed on CLI, it's always a dry run regardless of .env.
    if args.dry_run:
        logger.info("--dry-run flag detected — overriding settings.dry_run")

    if args.table == "customers":
        exit_code = run_customer_migration(args, settings)
    else:
        logger.error(f"Unknown table: {args.table}")
        exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
    