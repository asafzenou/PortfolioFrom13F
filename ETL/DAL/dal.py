from DataHandlers.DBDataHandler.db_abstract import AbstractDBHandler
from DataHandlers.WebDataFetcher import RemoteFileFetcher


class DAL:
    """
    Data Access Layer responsible only for database operations.
    Wraps a DB handler that implements AbstractDBHandler.
    """

    def __init__(self, db_handler: AbstractDBHandler):
        self.db_handler = db_handler
        self.web_fetcher = RemoteFileFetcher()

    def connect(self):
        self.db_handler.connect()

    def query(self, query: str):
        return self.db_handler.query(query)

    def execute(self, query: str, params=None):
        return self.db_handler.execute(query, params)

    def close(self):
        pass
        # self.db_handler.close()

    # ==================== REMOTE FILE OPERATIONS ====================

    def fetch_remote_stream(self, url: str, timeout: int = RemoteFileFetcher.DEFAULT_TIMEOUT):
        """
        Fetch streaming response from remote URL.

        Args:
            url: Remote URL to fetch.
            timeout: Request timeout in seconds.

        Returns:
            Response object with stream=True.
        """
        return self.web_fetcher.fetch_stream(url, timeout)

    def get_remote_file_size(self, response) -> int:
        """Get total file size from response."""
        return self.web_fetcher.get_total_size(response)


