import csv
from typing import List, Dict, Any

from Extractors.base_strategy import ExtractionStrategy


class CSVExtractionStrategy(ExtractionStrategy):
    """Extracts data from a CSV file"""

    def __init__(self, filepath: str):
        self.filepath = filepath

    def extract(self) -> List[Dict[str, Any]]:
        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                return list(reader)
        except FileNotFoundError:
            print(f"CSV file not found: {self.filepath}")
            return []
        except Exception as e:
            print(f"Error reading CSV: {e}")
            return []
