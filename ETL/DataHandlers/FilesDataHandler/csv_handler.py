import csv
from typing import List, Dict, Any


class CSVDataHandler():
    """Handler for CSV data operations"""
    def __init__(self, filepath: str):
        self.filepath = filepath

    def read(self) -> List[Dict[str, Any]]:
        """Read CSV file and return list of dictionaries"""
        data = []
        try:
            with open(self.filepath, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                data = list(reader)
        except FileNotFoundError:
            print(f"CSV file not found: {self.filepath}")
        return data

    def write(self, data: List[Dict[str, Any]]):
        """Write list of dictionaries to CSV file"""
        if not data:
            return
        try:
            with open(self.filepath, 'w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
        except IOError as e:
            print(f"Error writing CSV file: {e}")
