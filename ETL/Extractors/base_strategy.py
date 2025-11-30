from abc import ABC, abstractmethod
from typing import Any, List

class ExtractionStrategy(ABC):
    """
    Base class for ALL extraction strategies.
    Each extractor returns a list of data items (rows/records)
    """

    @abstractmethod
    def extract(self) -> List[Any]:
        pass
