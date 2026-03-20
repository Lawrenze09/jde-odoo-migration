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
    parser.add_argument(
        "--sync",
        action="store_true",
        help="Run incremental sync instead of full migration. "
            "Only processes records updated since last run.",
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
        # ── Stage 1: Extract ─────────────────────────────────────────
        logger.info("STAGE 1 — Extract")

        if args.source == "mock":
            extractor = MockExtractor()
        else:
            logger.error("Oracle source not yet implemented. Use --source mock.")
            return 1

        records = extractor.extract()

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
        # CLI flag takes full precedence over .env DRY_RUN setting
        is_dry_run = args.dry_run

        # Initialize load_result_for_report to None — stays None for dry runs.
        # Only populated for live runs so the report and summary log can use it.
        load_result_for_report = None

        logger.info("STAGE 4 — Load")

        if is_dry_run:
            logger.info("DRY RUN mode — writing preview CSV, skipping Odoo")
            loader     = CsvLoader()
            valid_path = loader.load(valid_records)
            failed_path = loader.load_failed(failed_records)
            if valid_path:
                logger.info(f"Dry run preview: {valid_path}")
            if failed_path:
                logger.info(f"Failed records:  {failed_path}")
        else:
            from loaders.odoo_loader import OdooLoader
            loader                 = OdooLoader()
            load_result            = loader.load(valid_records)
            load_result_for_report = load_result
            if load_result.failed > 0:
                logger.error(
                    f"Batch stopped — {load_result.failed} record(s) failed. "
                    f"batch_id: {load_result.batch_id}"
                )

        # ── Stage 5: Report ───────────────────────────────────────────
        if args.report:
            logger.info("STAGE 5 — Report")
            from reports.migration_report import MigrationReport
            report      = MigrationReport()
            report_path = report.generate(
                valid_records=valid_records,
                failed_records=failed_records,
                dry_run=is_dry_run,
                source=args.source,
                load_result=load_result_for_report,
            )
            logger.info(f"Report generated: {report_path}")

        # ── Summary log ───────────────────────────────────────────────
        # Use load result counts for live runs, validation counts for dry runs.
        # load_result_for_report is None for dry runs — guard before accessing.
        if load_result_for_report and not is_dry_run:
            success_count = load_result_for_report.loaded
            success_rate  = (success_count / len(records) * 100) if records else 0
        else:
            success_count = len(valid_records)
            success_rate  = (success_count / len(records) * 100) if records else 0

        logger.info("=" * 60)
        logger.info("MIGRATION COMPLETE")
        logger.info(f"Total extracted: {len(records)}")
        logger.info(f"Valid records:   {len(valid_records)}")
        logger.info(f"Failed records:  {len(failed_records)}")
        if load_result_for_report and not is_dry_run:
            logger.info(f"Created in Odoo: {load_result_for_report.loaded}")
            logger.info(f"Skipped:         {load_result_for_report.skipped}")
        logger.info(f"Success rate:    {success_rate:.1f}%")
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
    parser   = build_parser()
    args     = parser.parse_args()
    settings = get_settings()

    if args.dry_run:
        logger.info("--dry-run flag detected — overriding settings.dry_run")

    if args.sync:
        from sync.sync_engine import SyncEngine, SyncOutcome
        engine = SyncEngine(
            source=args.source,
            dry_run=args.dry_run,
            generate_report=args.report,
            limit=args.limit,
        )
        sync_result = engine.run()
        # Map outcome to exit code — schedulers need a simple pass/fail
        failed_outcomes = {SyncOutcome.FAILED, SyncOutcome.PARTIAL}
        sys.exit(1 if sync_result.outcome in failed_outcomes else 0)

    if args.table == "customers":
        exit_code = run_customer_migration(args, settings)
    else:
        logger.error(f"Unknown table: {args.table}")
        exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
    