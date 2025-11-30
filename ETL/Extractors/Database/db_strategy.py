from Extractors.base_strategy import ExtractionStrategy

class DBExtractionStrategy(ExtractionStrategy):
    """
    Extracts data from a database using a DAL-provided DB handler
    """

    def __init__(self, query: str, db_handler):
        self.query = query
        self.db_handler = db_handler

    def extract(self):
        self.db_handler.connect()
        return self.db_handler.query(self.query)
