from typing import Any, List
from DataHandlers.FilesDataHandler.xml_handler import XMLDataHandler
from .base_strategy import ExtractionStrategy


class XMLExtractionStrategy(ExtractionStrategy):
    def __init__(self, filepath: str):
        self.filepath = filepath

    def extract(self) -> List[Any]:
        handler = XMLDataHandler(self.filepath)
        return handler.read()
