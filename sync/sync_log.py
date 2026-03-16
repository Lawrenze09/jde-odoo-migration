"""
sync/sync_log.py

Role in pipeline: Tracks the watermark for each table after every sync run.
Stores the last UPMJ (Julian date) and UPMT (time in seconds) processed
per table so the incremental extractor knows where to start the next run.

Uses SQLite — same database as the transaction log for simplicity.
Each table gets its own watermark row, updated after every successful run.

Watermark strategy:
    Extract records where:
        UPMJ > last_upmj
        OR (UPMJ == last_upmj AND UPMT > last_upmt)
    This catches all records updated after the last sync timestamp,
    including multiple updates on the same Julian date.

Safety lag:
    On first run (no watermark), extracts all records.
    After each run, stores the maximum UPMJ/UPMT seen in that batch.
"""

import sqlite3
import os
from datetime import datetime
from dataclasses import dataclass
from utils.logger import get_logger

logger = get_logger(__name__)

# Default SQLite path — same file as transaction log for simplicity
DEFAULT_DB_PATH = "logs/transaction_log.db"


@dataclass
class SyncWatermark:
    """
    Watermark for a single table — the point to resume from on next run.

    Attributes:
        table_name:  JDE table name e.g. 'F0101'
        last_upmj:   Julian date of last processed record (0 = never synced)
        last_upmt:   Time in seconds of last processed record (0 = never synced)
        last_run_at: ISO timestamp of when the sync completed
        records_synced: How many records were processed in the last run
    """
    table_name:     str
    last_upmj:      int
    last_upmt:      int
    last_run_at:    str
    records_synced: int


class SyncLog:
    """
    Reads and writes sync watermarks for incremental extraction.
    One row per JDE table — updated after every successful pipeline run.
    """

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        """
        Initialize the SyncLog and create the watermark table if needed.

        Args:
            db_path (str): Path to the SQLite database.
                           Shared with transaction log by default.
        """
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_table()
        logger.info(f"SyncLog initialized | path: {db_path}")

    def _init_table(self):
        """
        Create the sync_watermark table if it does not exist.
        UNIQUE(table_name) ensures one watermark row per JDE table.
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sync_watermark (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name     TEXT    NOT NULL UNIQUE,
                    last_upmj      INTEGER NOT NULL DEFAULT 0,
                    last_upmt      INTEGER NOT NULL DEFAULT 0,
                    last_run_at    TEXT,
                    records_synced INTEGER NOT NULL DEFAULT 0
                )
            """)
            conn.commit()

    def get_watermark(self, table_name: str) -> SyncWatermark:
        """
        Get the current watermark for a table.
        Returns a zero watermark if the table has never been synced —
        this causes the extractor to return all records on first run.

        Args:
            table_name (str): JDE table name e.g. 'F0101'

        Returns:
            SyncWatermark: Current watermark. last_upmj=0 means never synced.
        """
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT table_name, last_upmj, last_upmt,
                       last_run_at, records_synced
                FROM sync_watermark
                WHERE table_name = ?
                """,
                (table_name,)
            ).fetchone()

        if row:
            return SyncWatermark(
                table_name=row[0],
                last_upmj=row[1],
                last_upmt=row[2],
                last_run_at=row[3],
                records_synced=row[4],
            )

        # No watermark yet — first run, extract everything
        logger.info(
            f"No watermark found for {table_name} — "
            f"first run, will extract all records"
        )
        return SyncWatermark(
            table_name=table_name,
            last_upmj=0,
            last_upmt=0,
            last_run_at=None,
            records_synced=0,
        )

    def update_watermark(
        self,
        table_name: str,
        last_upmj: int,
        last_upmt: int,
        records_synced: int,
    ):
        """
        Update the watermark after a successful sync run.
        Uses INSERT OR REPLACE so first run creates the row,
        subsequent runs update it.

        Args:
            table_name:     JDE table name e.g. 'F0101'
            last_upmj:      Maximum UPMJ value seen in this run
            last_upmt:      Maximum UPMT value seen in this run
                            (only meaningful when UPMJ == last_upmj)
            records_synced: Number of records processed in this run
        """
        run_at = datetime.now().isoformat()

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO sync_watermark
                    (table_name, last_upmj, last_upmt,
                     last_run_at, records_synced)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(table_name) DO UPDATE SET
                    last_upmj      = excluded.last_upmj,
                    last_upmt      = excluded.last_upmt,
                    last_run_at    = excluded.last_run_at,
                    records_synced = excluded.records_synced
                """,
                (table_name, last_upmj, last_upmt, run_at, records_synced)
            )
            conn.commit()

        logger.info(
            f"Watermark updated | table: {table_name} | "
            f"last_upmj: {last_upmj} | last_upmt: {last_upmt} | "
            f"records: {records_synced}"
        )

    def get_all_watermarks(self) -> list[SyncWatermark]:
        """
        Return watermarks for all tracked tables.
        Used for reporting and debugging sync state.

        Returns:
            list[SyncWatermark]: One entry per synced table.
        """
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT table_name, last_upmj, last_upmt,
                       last_run_at, records_synced
                FROM sync_watermark
                ORDER BY table_name
                """
            ).fetchall()

        return [
            SyncWatermark(
                table_name=row[0],
                last_upmj=row[1],
                last_upmt=row[2],
                last_run_at=row[3],
                records_synced=row[4],
            )
            for row in rows
        ]
    