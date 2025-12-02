import pandas as pd
import os
from datetime import datetime
from typing import Optional, List
from Logging.logger import ETLLogger


class DataLoader:
    """Handles saving processed data to various formats."""

    def __init__(
        self, output_dir: str = "13f_outputs", logger: Optional[ETLLogger] = None
    ):
        self.output_dir = output_dir
        self.logger = logger or ETLLogger(name="DataLoader")
        os.makedirs(output_dir, exist_ok=True)

    # ==================== MAIN ORCHESTRATION ====================

    def load(
        self, df: pd.DataFrame, filename: str, formats: Optional[List[str]] = None) -> None:
        """Main orchestration function to save data to multiple formats."""
        pass

    def save_summary_report(self, df: pd.DataFrame, filename: str) -> str:
        """Save dataset summary report to file."""
        report = self.get_summary_report(df)
        filepath = os.path.join(self.output_dir, f"{filename}_summary.txt")

        with open(filepath, "w") as f:
            f.write(report)

        self.logger.info(f"Saved Report: {filepath}")
        return filepath
