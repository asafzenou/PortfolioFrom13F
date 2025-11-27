from typing import Any, List
from DataHandlers.FilesDataHandler.csv_handler import CSVDataHandler
from .base_strategy import ExtractionStrategy


class CSVExtractionStrategy(ExtractionStrategy):
    def __init__(self, filepath: str):
        self.filepath = filepath

    def extract(self) -> List[Any]:
        handler = CSVDataHandler(self.filepath)
        return handler.read()
