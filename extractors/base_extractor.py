"""
extractors/base_extractor.py

Role in pipeline: Defines the extraction contract for all data sources.
Any class that pulls data into the pipeline — whether from a CSV file,
Oracle JDE database, or any future source — must inherit from this class
and implement extract().

This guarantees that the transformer, validator, and loader never need
to know which source they are processing. They call extract() and receive
the same structure regardless of source.

Supports both full load and incremental sync:
    Full load:   extract(last_upmj=0, last_upmt=0) — returns all records
    Incremental: extract(last_upmj=X, last_upmt=Y) — returns only newer records

Input:  Depends on the concrete implementation (file path, DB connection)
Output: list[dict] where each dict is one raw JDE record with column names
        as keys: AN8, ALPH, AT1, PH1, ADD1, ADD2, CTY1, ADDS, ADDZ,
        COUN, TAX, PA8, UPMJ, UPMT
"""

from abc import ABC, abstractmethod


class BaseExtractor(ABC):
    """
    Abstract base class for all data extractors in the migration pipeline.

    Subclasses must implement extract(). Python will refuse to instantiate
    any subclass that does not implement this method — the contract is
    enforced at object creation time, not at runtime.

    Concrete implementations:
        MockExtractor  — reads from mock_data/F0101.csv (development)
        JdeExtractor   — connects to Oracle JDE database (Phase 4)
    """

    @abstractmethod
    def extract(
        self,
        last_upmj: int = 0,
        last_upmt: int = 0,
    ) -> list[dict]:
        """
        Extract records from the data source.

        When last_upmj=0 and last_upmt=0, returns all records (full load).
        When watermark values are provided, returns only records updated
        after that point — incremental sync mode.

        Filter logic for incremental mode:
            UPMJ > last_upmj
            OR (UPMJ == last_upmj AND UPMT > last_upmt)

        This correctly handles multiple updates on the same Julian date
        by using UPMT as a tiebreaker within the same day.

        Args:
            last_upmj (int): Julian date watermark from previous run.
                             0 means first run — return all records.
            last_upmt (int): Time in seconds watermark from previous run.
                             0 means first run — return all records.

        Returns:
            list[dict]: Raw JDE records. Each dict represents one row
                        from the F0101 Address Book table. Keys match
                        JDE column names exactly. Example:
                        {
                            "AN8":  "1001",
                            "ALPH": "Limketkai Center Inc",
                            "AT1":  "C",
                            "PH1":  "+63 82 234 5678",
                            "ADD1": "Limketkai Drive",
                            "CTY1": "Cagayan de Oro",
                            "UPMJ": "126072",
                            "UPMT": "28800",
                            ...
                        }

        Raises:
            NotImplementedError: Implicitly, if subclass does not
                implement this method (enforced by @abstractmethod).
        """
        pass
    