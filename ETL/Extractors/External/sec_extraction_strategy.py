import requests
import zipfile
import xml.etree.ElementTree as ET
import glob
import os
import tempfile
from typing import List, Dict, Any

from Extractors.base_strategy import ExtractionStrategy


class SECExtractionStrategy(ExtractionStrategy):
    """
    Extracts 13F filings from SEC dataset ZIPs.
    Downloads ZIP → extracts → parses XML files → returns combined records.

    The ZIP contains ALL 13F filings for the date range across the entire SEC.
    No CIK filtering needed — parse everything.
    """

    def __init__(
        self,
        sec_zip_url: str,
        output_dir: str = "13f_outputs",
    ):
        self.sec_zip_url = sec_zip_url
        self.output_dir = output_dir
        self.namespace = {"n1": "http://www.sec.gov/edgar/thirteenffiler"}

    def _download_zip(self, url: str, output_path: str) -> None:
        """Downloads SEC dataset ZIP with proper headers."""
        headers = {"User-Agent": "AsafZenou-Research/1.0"}
        print(f"Downloading from: {url}")
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size:
                    pct = (downloaded / total_size) * 100
                    print(f"  {pct:.1f}% downloaded...")

        print(f"✓ Downloaded: {output_path}")

    def _extract_zip(self, zip_path: str, extract_to: str) -> None:
        """Extracts ZIP to target directory."""
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(extract_to)
        print(f"✓ Extracted to: {extract_to}")

    def _parse_13f_xml(self, xml_file: str) -> List[Dict[str, Any]]:
        """Parses single 13F XML file and returns list of holdings."""
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            info_table = root.findall(".//n1:infoTable", self.namespace)

            rows = []
            for entry in info_table:
                data = {
                    "cusip": entry.findtext(
                        "n1:cusip", default="", namespaces=self.namespace
                    ),
                    "nameOfIssuer": entry.findtext(
                        "n1:nameOfIssuer", default="", namespaces=self.namespace
                    ),
                    "value": entry.findtext(
                        "n1:value", default="", namespaces=self.namespace
                    ),
                    "shares": entry.findtext(
                        "n1:shrsOrPrnAmt/n1:sshPrnamt",
                        default="",
                        namespaces=self.namespace,
                    ),
                    "share_type": entry.findtext(
                        "n1:shrsOrPrnAmt/n1:sshPrnamtType",
                        default="",
                        namespaces=self.namespace,
                    ),
                    "investment_discretion": entry.findtext(
                        "n1:investmentDiscretion", default="", namespaces=self.namespace
                    ),
                    "xml_source": xml_file,
                }
                rows.append(data)

            return rows
        except Exception as e:
            print(f"⚠ Error parsing {xml_file}: {e}")
            return []

    def _parse_folder(self, folder: str) -> List[Dict[str, Any]]:
        """Parses all XML files in folder recursively."""
        all_rows = []
        xml_files = glob.glob(f"{folder}/**/*.xml", recursive=True)
        print(f"Found {len(xml_files)} XML files to parse")

        for i, xml_file in enumerate(xml_files, 1):
            if i % 100 == 0:
                print(f"  Parsed {i}/{len(xml_files)} files...")
            rows = self._parse_13f_xml(xml_file)
            all_rows.extend(rows)

        return all_rows

    def extract(self) -> List[Dict[str, Any]]:
        """
        Main extraction flow:
        1. Download SEC ZIP (all 13F filings for date range)
        2. Extract to temp directory
        3. Parse all XML files
        4. Return combined records
        """
        if not self.sec_zip_url:
            raise ValueError("sec_zip_url is required for SEC ZIP extraction")

        temp_dir = tempfile.mkdtemp(prefix="sec_13f_")
        os.makedirs(self.output_dir, exist_ok=True)

        try:
            # Step 1: Download
            zip_path = os.path.join(self.output_dir, "sec_13f_data.zip")
            self._download_zip(self.sec_zip_url, zip_path)

            # Step 2: Extract
            extract_dir = os.path.join(temp_dir, "extracted")
            self._extract_zip(zip_path, extract_dir)

            # Step 3: Parse all XMLs
            print("Parsing XML files...")
            combined_rows = self._parse_folder(extract_dir)
            print(f"✓ Parsed {len(combined_rows)} holdings")

            return combined_rows

        finally:
            import shutil

            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
