"""
reports/migration_report.py

Role in pipeline: Final stage output — generates an Excel reconciliation
report that a non-technical business owner can read and trust. Three sheets:
  Sheet 1 — Summary: counts, success rate, run mode, timestamp
  Sheet 2 — Successful Records: every valid record that was or would be loaded
  Sheet 3 — Failed Records: every rejected record with plain English reason

Input:  valid_records and failed_records from CustomerValidator
        load_result from OdooLoader (None for dry runs)
Output: output/migration_report_YYYYMMDD_HHMMSS.xlsx

Uses openpyxl for Excel formatting — column widths, bold headers,
color coding, and Text number format to prevent scientific notation.
"""

import os
from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter
from utils.logger import get_logger

logger = get_logger(__name__)

# ── Color constants ─────────────────────────────────────────────────────────
HEADER_GREEN = "1F7A4A"
HEADER_RED   = "C0392B"
HEADER_BLUE  = "1A3A5C"
ROW_GREEN    = "EAF7EE"
ROW_RED      = "FDEDEC"
ROW_ALT      = "F7F9FC"

# ── Column definitions ──────────────────────────────────────────────────────
VALID_COLUMNS = [
    ("JDE AN8",       "_jde_an8",      12),
    ("Name",          "name",          30),
    ("Phone",         "phone",         18),
    ("Street",        "street",        35),
    ("Street 2",      "street2",       25),
    ("City",          "city",          20),
    ("Zip",           "zip",           10),
    ("State Code",    "state_code",    14),
    ("Country Code",  "country_code",  14),
    ("VAT / TIN",     "vat",           20),
    ("Customer Rank", "customer_rank", 14),
    ("Is Company",    "is_company",    12),
    ("Parent AN8",    "parent_an8",    12),
    ("Comment",       "comment",       50),
]

FAILED_COLUMNS = [
    ("JDE AN8",        "_jde_an8",         12),
    ("Name",           "name",             30),
    ("Phone",          "phone",            18),
    ("Street",         "street",           35),
    ("City",           "city",             20),
    ("VAT / TIN",      "vat",              20),
    ("Failed Rule",    "_failed_rule",     28),
    ("Failure Reason", "_failure_reason",  60),
]

# Columns that must be Text format to prevent scientific notation in Excel
TEXT_FORMAT_COLUMNS = {"phone", "vat", "zip", "_jde_an8", "parent_an8"}


class MigrationReport:
    """
    Generates a formatted Excel reconciliation report from migration results.
    One report per pipeline run — timestamped so runs are never overwritten.
    """

    def __init__(self, output_dir: str = "output"):
        """
        Initialize the report generator.

        Args:
            output_dir (str): Directory to write report files.
                              Created automatically if it does not exist.
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def generate(
        self,
        valid_records: list[dict],
        failed_records: list[dict],
        dry_run: bool = True,
        source: str = "mock",
        load_result=None,
    ) -> str:
        """
        Generate the full three-sheet Excel migration report.

        Args:
            valid_records  (list[dict]): Valid records from CustomerValidator.
            failed_records (list[dict]): Failed records from CustomerValidator.
            dry_run        (bool):       True if this was a dry run.
            source         (str):        Data source label e.g. 'mock' or 'oracle'.
            load_result:                 LoadResult from OdooLoader, or None for dry runs.

        Returns:
            str: Full path to the generated .xlsx file.
        """
        wb = Workbook()

        ws_summary = wb.active
        ws_summary.title = "Summary"
        ws_valid   = wb.create_sheet("Successful Records")
        ws_failed  = wb.create_sheet("Failed Records")

        # Pass load_result through to the summary sheet builder
        self._build_summary_sheet(
            ws_summary, valid_records, failed_records,
            dry_run, source, load_result
        )
        self._build_valid_sheet(ws_valid, valid_records)
        self._build_failed_sheet(ws_failed, failed_records)

        output_path = self._build_output_path()
        wb.save(output_path)

        logger.info(
            f"Migration report generated | "
            f"valid: {len(valid_records)} | "
            f"failed: {len(failed_records)} | "
            f"path: {output_path}"
        )
        return output_path

    # ── Sheet builders ──────────────────────────────────────────────────────

    def _build_summary_sheet(self, ws, valid, failed, dry_run, source, load_result):
        """
        Build Sheet 1 — Summary with counts, success rate, and run metadata.

        Args:
            ws:          openpyxl Worksheet object
            valid:       list of valid records
            failed:      list of failed records
            dry_run:     bool — whether this was a dry run
            source:      str — data source label
            load_result: LoadResult from OdooLoader, or None for dry runs
        """
        run_mode = "DRY RUN — No data written to Odoo" if dry_run else "LIVE RUN — Data written to Odoo"
        run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        total    = len(valid) + len(failed)

        # ── Title ──
        ws.merge_cells("A1:C1")
        title_cell            = ws["A1"]
        title_cell.value      = "JDE to Odoo Migration Report"
        title_cell.font       = Font(bold=True, size=16, color="FFFFFF")
        title_cell.fill       = PatternFill("solid", fgColor=HEADER_BLUE)
        title_cell.alignment  = Alignment(horizontal="center", vertical="center")
        ws.row_dimensions[1].height = 32

        # ── Calculate correct success rate ──
        # Live run: success = records actually created in Odoo
        # Dry run:  success = records that passed validation (best proxy)
        if load_result and not dry_run:
            loaded_count      = load_result.loaded
            odoo_failed_count = load_result.failed
            not_processed     = load_result.not_processed
            skipped_count     = load_result.skipped
            success_rate      = (loaded_count / total * 100) if total > 0 else 0
        else:
            loaded_count      = None
            odoo_failed_count = None
            not_processed     = None
            skipped_count     = None
            success_rate      = (len(valid) / total * 100) if total > 0 else 0

        # ── Build summary rows — Success Rate appears ONCE at the end ──
        summary_rows = [
            ("Run Mode",       run_mode),
            ("Data Source",    source.upper()),
            ("Run Timestamp",  run_time),
            ("",               ""),
            ("Total Extracted", total),
            ("",               ""),
            ("— Validation —", ""),
            ("Valid Records",  len(valid)),
            ("Failed Records", len(failed)),
            ("",               ""),
        ]

        # Odoo load section — only for live runs with a load result
        if load_result and not dry_run:
            summary_rows += [
                ("— Odoo Load —",          ""),
                ("Created in Odoo",        loaded_count),
                ("Rejected by Odoo",       odoo_failed_count),
                ("Not Processed",          not_processed),
                ("Skipped (prev. loaded)", skipped_count),
                ("",                       ""),
            ]

        # Success Rate always last — calculated correctly per run type above
        summary_rows.append(("Success Rate", f"{success_rate:.1f}%"))

        for i, (label, value) in enumerate(summary_rows, start=2):
            label_cell = ws.cell(row=i, column=1, value=label)
            value_cell = ws.cell(row=i, column=2, value=value)
            label_cell.font = Font(bold=True)

            if label == "Valid Records":
                value_cell.fill = PatternFill("solid", fgColor=ROW_GREEN)
                value_cell.font = Font(bold=True, color="1F7A4A")
            elif label == "Failed Records" and len(failed) > 0:
                value_cell.fill = PatternFill("solid", fgColor=ROW_RED)
                value_cell.font = Font(bold=True, color="C0392B")
            elif label == "Created in Odoo" and loaded_count is not None:
                value_cell.fill = PatternFill("solid", fgColor=ROW_GREEN)
                value_cell.font = Font(bold=True, color="1F7A4A")
            elif label == "Rejected by Odoo" and odoo_failed_count:
                value_cell.fill = PatternFill("solid", fgColor=ROW_RED)
                value_cell.font = Font(bold=True, color="C0392B")
            elif label == "Success Rate":
                # If all records were skipped (re-run with no new data),
                # show a neutral note instead of a misleading 0.0%
                if load_result and not dry_run and load_result.skipped == total and load_result.loaded == 0:
                    value_cell.value = "All records already migrated"
                    value_cell.font = Font(bold=True, color="1A3A5C")
                else:
                    value_cell.font = Font(bold=True, size=12)
            elif label == "Run Mode":
                color = "FEF9E7" if dry_run else "EAF7EE"
                value_cell.fill = PatternFill("solid", fgColor=color)
                value_cell.font = Font(
                    bold=True,
                    color="B7770D" if dry_run else "1F7A4A"
                )

        ws.column_dimensions["A"].width = 25
        ws.column_dimensions["B"].width = 45

    def _build_valid_sheet(self, ws, valid_records):
        """
        Build Sheet 2 — Successful Records with all Odoo fields.

        Args:
            ws:            openpyxl Worksheet object
            valid_records: list of valid transformed records
        """
        self._write_sheet(
            ws,
            records=valid_records,
            columns=VALID_COLUMNS,
            header_color=HEADER_GREEN,
            row_fill=ROW_GREEN,
        )

    def _build_failed_sheet(self, ws, failed_records):
        """
        Build Sheet 3 — Failed Records with failure rule and reason.

        Args:
            ws:             openpyxl Worksheet object
            failed_records: list of failed records with _failed_rule and _failure_reason
        """
        self._write_sheet(
            ws,
            records=failed_records,
            columns=FAILED_COLUMNS,
            header_color=HEADER_RED,
            row_fill=ROW_RED,
        )

    # ── Shared sheet writer ─────────────────────────────────────────────────

    def _write_sheet(self, ws, records, columns, header_color, row_fill):
        """
        Write a formatted data sheet with bold colored headers and data rows.
        Applies Text number format to phone, vat, zip, and AN8 columns to
        prevent Excel from converting long numbers to scientific notation.

        Args:
            ws:           openpyxl Worksheet
            records:      list[dict] to write
            columns:      list of (header_label, dict_key, column_width) tuples
            header_color: hex color string for header row background
            row_fill:     hex color string for data row background
        """
        for col_idx, (header, _, width) in enumerate(columns, start=1):
            cell           = ws.cell(row=1, column=col_idx, value=header)
            cell.font      = Font(bold=True, color="FFFFFF")
            cell.fill      = PatternFill("solid", fgColor=header_color)
            cell.alignment = Alignment(horizontal="center", vertical="center")
            ws.column_dimensions[get_column_letter(col_idx)].width = width

        ws.row_dimensions[1].height = 22

        for row_idx, record in enumerate(records, start=2):
            fill_color = row_fill if row_idx % 2 == 0 else ROW_ALT
            for col_idx, (_, key, _) in enumerate(columns, start=1):
                value      = record.get(key)
                cell       = ws.cell(row=row_idx, column=col_idx, value=value)
                cell.fill  = PatternFill("solid", fgColor=fill_color)

                # Force Text format — prevents scientific notation and
                # preserves leading zeros on phone numbers and TIN fields
                if key in TEXT_FORMAT_COLUMNS:
                    cell.number_format = "@"
                    if value is not None:
                        cell.value = str(value)

        ws.freeze_panes = "A2"

    # ── Helpers ─────────────────────────────────────────────────────────────

    def _build_output_path(self) -> str:
        """
        Build a timestamped output file path for the Excel report.

        Returns:
            str: Full file path e.g. output/migration_report_20260313_201500.xlsx
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename  = f"migration_report_{timestamp}.xlsx"
        return os.path.join(self.output_dir, filename)
    