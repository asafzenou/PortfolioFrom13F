import csv
from typing import List, Dict, Any


class Loader:
    """Handles loading transformed data to destinations"""

    def load_to_csv(self, records: List[Dict[str, Any]], filepath: str) -> None:
        """Load records to CSV file"""
        if not records:
            return

        try:
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=records[0].keys())
                writer.writeheader()
                writer.writerows(records)
            print(f"[OK] Loaded {len(records)} records to {filepath}")
        except Exception as e:
            print(f"[ERROR] Failed to load CSV: {e}")

    def load_to_sql(self, records: List[Dict[str, Any]]) -> None:
        """Load records to SQL database"""
        print(f"[INFO] SQL loading not yet implemented for {len(records)} records")

    def load_to_graph(self, records: List[Dict[str, Any]]) -> None:
        """Load records to Graph database"""
        print(f"[INFO] Graph DB loading not yet implemented for {len(records)} records")
