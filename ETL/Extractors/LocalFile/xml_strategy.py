import xml.etree.ElementTree as ET
from typing import List, Dict, Any

from Extractors.base_strategy import ExtractionStrategy


class XMLExtractionStrategy(ExtractionStrategy):
    """Extracts data from an XML file"""

    def __init__(self, filepath: str):
        self.filepath = filepath

    def extract(self) -> List[Dict[str, Any]]:
        try:
            tree = ET.parse(self.filepath)
            root = tree.getroot()

            records = []
            for item in root.findall(".//record"):
                record = {}
                for child in item:
                    record[child.tag] = child.text
                records.append(record)

            return records
        except FileNotFoundError:
            print(f"XML file not found: {self.filepath}")
            return []
        except ET.ParseError as e:
            print(f"Error parsing XML: {e}")
            return []
