import logging
import os
from datetime import datetime
from typing import Optional


class ETLLogger:
    """Centralized logger handler for ETL pipeline."""

    LOG_DIR = "logs"
    LOG_LEVEL = logging.INFO

    def __init__(self, name: str = "ETL_Pipeline", log_dir: Optional[str] = None):
        """
        Initialize logger instance.

        Args:
            name: Logger name.
            log_dir: Directory to save logs (default: logs/).
        """
        self.name = name
        self.log_dir = log_dir or self.LOG_DIR
        os.makedirs(self.log_dir, exist_ok=True)

        self.logger = logging.getLogger(name)
        self.logger.setLevel(self.LOG_LEVEL)

        # Remove existing handlers to avoid duplicates
        self.logger.handlers.clear()

        # Create formatters
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # File handler - logs to file only (no console output)
        self._setup_file_handler(formatter)

    def _setup_file_handler(self, formatter: logging.Formatter) -> None:
        """Setup file handler for logger to file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(self.log_dir, f"etl_pipeline_{timestamp}.log")

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(self.LOG_LEVEL)
        file_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.log_file = log_file

    def info(self, message: str) -> None:
        """Log info message."""
        self.logger.info(message)

    def debug(self, message: str) -> None:
        """Log debug message."""
        self.logger.debug(message)

    def warning(self, message: str) -> None:
        """Log warning message."""
        self.logger.warning(message)

    def error(self, message: str) -> None:
        """Log error message."""
        self.logger.error(message)

    def critical(self, message: str) -> None:
        """Log critical message."""
        self.logger.critical(message)

    def exception(self, message: str) -> None:
        """Log exception with traceback."""
        self.logger.exception(message)

    def get_log_file(self) -> str:
        """Get path to current log file."""
        return self.log_file

    def close(self) -> None:
        """Close all handlers."""
        for handler in self.logger.handlers:
            handler.close()
            self.logger.removeHandler(handler)
