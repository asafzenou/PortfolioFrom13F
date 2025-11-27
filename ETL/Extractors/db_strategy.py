from typing import Any, List
from .base_strategy import ExtractionStrategy
from DataHandlers.DBDataHandler.db_abstract import AbstractDBHandler


class DBExtractionStrategy(ExtractionStrategy):
    def __init__(self, query: str, db_handler: AbstractDBHandler):
        self.query = query
        self.db_handler = db_handler

    def extract(self) -> List[Any]:
        self.db_handler.connect()
        return self.db_handler.query(self.query)
