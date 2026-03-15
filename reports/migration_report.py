"""
reports/migration_report.py

Role in pipeline: Final stage output — generates an Excel reconciliation
report that a non-technical business owner can read and trust. Three sheets:
  Sheet 1 — Summary: counts, success rate, run mode, timestamp
  Sheet 2 — Successful Records: every valid record that was or would be loaded
  Sheet 3 — Failed Records: every rejected record with plain English reason

Input:  valid_records and failed_records from CustomerValidator
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

# ── Color constants ────────────────────────────────────────────────────────────
# Header row background colors — dark enough for white text
HEADER_GREEN  = "1F7A4A"   # Sheet 2 header — success
HEADER_RED    = "C0392B"   # Sheet 3 header — failed
HEADER_BLUE   = "1A3A5C"   # Sheet 1 header — summary

# Row fill colors for data rows
ROW_GREEN     = "EAF7EE"   # Light green — valid record rows
ROW_RED       = "FDEDEC"   # Light red   — failed record rows
ROW_ALT       = "F7F9FC"   # Alternating row fill for readability

# ── Column definitions ─────────────────────────────────────────────────────────
# Columns for Sheet 2 (valid records) — order controls Excel column order
VALID_COLUMNS = [
    ("JDE AN8",        "_jde_an8",      12),
    ("Name",           "name",          30),
    ("Phone",          "phone",         18),
    ("Street",         "street",        35),
    ("Street 2",       "street2",       25),
    ("City",           "city",          20),
    ("Zip",            "zip",           10),
    ("State Code",     "state_code",    14),
    ("Country Code",   "country_code",  14),
    ("VAT / TIN",      "vat",           20),
    ("Customer Rank",  "customer_rank", 14),
    ("Is Company",     "is_company",    12),
    ("Parent AN8",     "parent_an8",    12),
    ("Comment",        "comment",       50),
]

# Columns for Sheet 3 (failed records)
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

# Columns that must be formatted as Text to prevent scientific notation
# Excel auto-converts long number strings to scientific notation otherwise
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
    ) -> str:
        """
        Generate the full three-sheet Excel migration report.

        Args:
            valid_records  (list[dict]): Valid records from CustomerValidator.
            failed_records (list[dict]): Failed records from CustomerValidator.
            dry_run        (bool):       True if this was a dry run — shown in summary.
            source         (str):        Data source label e.g. 'mock' or 'oracle'.

        Returns:
            str: Full path to the generated .xlsx file.
        """
        wb = Workbook()

        # openpyxl creates a default sheet — rename it to Summary
        ws_summary = wb.active
        ws_summary.title = "Summary"

        ws_valid  = wb.create_sheet("Successful Records")
        ws_failed = wb.create_sheet("Failed Records")

        self._build_summary_sheet(
            ws_summary, valid_records, failed_records, dry_run, source
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

    # ── Sheet builders ─────────────────────────────────────────────────────────

    def _build_summary_sheet(self, ws, valid, failed, dry_run, source):
        """
        Build Sheet 1 — Summary with counts, success rate, and run metadata.

        Args:
            ws:       openpyxl Worksheet object
            valid:    list of valid records
            failed:   list of failed records
            dry_run:  bool — whether this was a dry run
            source:   str — data source label
        """
        total     = len(valid) + len(failed)
        success_rate = (len(valid) / total * 100) if total > 0 else 0
        run_mode  = "DRY RUN — No data written to Odoo" if dry_run else "LIVE RUN — Data written to Odoo"
        run_time  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ── Title ──
        ws.merge_cells("A1:C1")
        title_cell = ws["A1"]
        title_cell.value = "JDE to Odoo Migration Report"
        title_cell.font      = Font(bold=True, size=16, color="FFFFFF")
        title_cell.fill      = PatternFill("solid", fgColor=HEADER_BLUE)
        title_cell.alignment = Alignment(horizontal="center", vertical="center")
        ws.row_dimensions[1].height = 32

        # ── Summary rows ──
        summary_rows = [
            ("Run Mode",          run_mode),
            ("Data Source",       source.upper()),
            ("Run Timestamp",     run_time),
            ("",                  ""),
            ("Total Extracted",   total),
            ("Valid Records",     len(valid)),
            ("Failed Records",    len(failed)),
            ("Success Rate",      f"{success_rate:.1f}%"),
        ]

        for i, (label, value) in enumerate(summary_rows, start=2):
            label_cell = ws.cell(row=i, column=1, value=label)
            value_cell = ws.cell(row=i, column=2, value=value)

            # Bold the labels
            label_cell.font = Font(bold=True)

            # Color code the key metric rows
            if label == "Valid Records":
                value_cell.fill = PatternFill("solid", fgColor=ROW_GREEN)
                value_cell.font = Font(bold=True, color="1F7A4A")
            elif label == "Failed Records" and len(failed) > 0:
                value_cell.fill = PatternFill("solid", fgColor=ROW_RED)
                value_cell.font = Font(bold=True, color="C0392B")
            elif label == "Success Rate":
                value_cell.font = Font(bold=True, size=12)
            elif label == "Run Mode":
                # Highlight dry run in amber, live run in green
                color = "FEF9E7" if dry_run else "EAF7EE"
                value_cell.fill = PatternFill("solid", fgColor=color)
                value_cell.font = Font(
                    bold=True,
                    color="B7770D" if dry_run else "1F7A4A"
                )

        ws.column_dimensions["A"].width = 22
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

    # ── Shared sheet writer ────────────────────────────────────────────────────

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
        # ── Header row ──
        for col_idx, (header, _, width) in enumerate(columns, start=1):
            cell = ws.cell(row=1, column=col_idx, value=header)
            cell.font      = Font(bold=True, color="FFFFFF")
            cell.fill      = PatternFill("solid", fgColor=header_color)
            cell.alignment = Alignment(horizontal="center", vertical="center")
            ws.column_dimensions[get_column_letter(col_idx)].width = width

        ws.row_dimensions[1].height = 22

        # ── Data rows ──
        for row_idx, record in enumerate(records, start=2):
            # Alternate row fills for readability — every other row is slightly shaded
            fill_color = row_fill if row_idx % 2 == 0 else ROW_ALT

            for col_idx, (_, key, _) in enumerate(columns, start=1):
                value = record.get(key)
                cell  = ws.cell(row=row_idx, column=col_idx, value=value)
                cell.fill = PatternFill("solid", fgColor=fill_color)

                # Force Text format on columns that contain long numeric strings.
                # Without this, Excel converts 123456789000 → 1.23E+11 and
                # 09171234567 → 9171234567 (drops leading zero).
                if key in TEXT_FORMAT_COLUMNS:
                    cell.number_format = "@"
                    # Re-set value as string to ensure Excel treats it as text
                    if value is not None:
                        cell.value = str(value)

        # Freeze the header row so it stays visible when scrolling
        ws.freeze_panes = "A2"

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _build_output_path(self) -> str:
        """
        Build a timestamped output file path for the Excel report.

        Returns:
            str: Full file path e.g. output/migration_report_20260313_201500.xlsx
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename  = f"migration_report_{timestamp}.xlsx"
        return os.path.join(self.output_dir, filename)
    