from abc import ABC, abstractmethod
from typing import List, Any


class ExtractionStrategy(ABC):
    """Base Strategy interface for extractors."""

    @abstractmethod
    def extract(self) -> List[Any]:
        pass
