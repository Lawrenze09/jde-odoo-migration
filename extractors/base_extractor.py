from abc import ABC, abstractmethod


class BaseExtractor(ABC):

    @abstractmethod
    def extract(self) -> list[dict]:
        """
        Extract records from a data source.
        Returns a list of dicts where each dict is one raw JDE record.
        Keys must match JDE F0101 column names: AN8, ALPH, AT1, etc.
        """
        pass
    