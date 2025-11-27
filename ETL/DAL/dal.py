from DataHandlers.DBDataHandler.db_abstract import AbstractDBHandler


class DAL:
    """
    Data Access Layer responsible only for database operations.
    Wraps a DB handler that implements AbstractDBHandler.
    """

    def __init__(self, db_handler: AbstractDBHandler):
        self.db_handler = db_handler

    def connect(self):
        self.db_handler.connect()

    def query(self, query: str):
        return self.db_handler.query(query)

    def execute(self, query: str, params=None):
        return self.db_handler.execute(query, params)

    def close(self):
        self.db_handler.close()
