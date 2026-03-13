"""
extractors/base_extractor.py

Role in pipeline: Defines the extraction contract for all data sources.
Any class that pulls data into the pipeline — whether from a CSV file,
Oracle JDE database, or any future source — must inherit from this class
and implement extract().

This guarantees that the transformer, validator, and loader never need
to know which source they are processing. They call extract() and receive
the same structure regardless of source.

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
    def extract(self) -> list[dict]:
        """
        Extract records from the data source.

        Returns a list of raw JDE records. Each dict represents one row
        from the F0101 Address Book table. Keys match JDE column names
        exactly — no transformation or validation is done here.
        That is the transformer's and validator's job.

        Returns:
            list[dict]: Raw JDE records. Example single record:
                {
                    "AN8": 1001,
                    "ALPH": "Limketkai Center Inc",
                    "AT1": "C",
                    "PH1": "+63 82 234 5678",
                    "ADD1": "Limketkai Drive",
                    "CTY1": "Cagayan de Oro",
                    ...
                }

        Raises:
            NotImplementedError: Implicitly, if subclass does not
                implement this method (enforced by @abstractmethod).
        """
        pass