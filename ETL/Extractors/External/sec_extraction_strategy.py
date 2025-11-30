import requests
import zipfile
import csv
import glob
import os
import tempfile
import shutil
from typing import List, Dict, Any, Optional
from datetime import datetime

from Extractors.base_strategy import ExtractionStrategy


class SECExtractionStrategy(ExtractionStrategy):
    """
    Extracts 13F filings from SEC quarterly dataset ZIPs.
    Downloads quarterly ZIPs → extracts TSV files → parses → combines records.

    Quarterly datasets from: https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets
    Example: https://www.sec.gov/files/structureddata/data/form-13f-data-sets/01jun2025-31aug2025_form13f.zip
    """

    # Quarterly dataset mappings (date_range -> zip_filename_pattern)
    QUARTERLY_DATASETS = {
        "2025_Q2": "01jun2025-31aug2025_form13f.zip",
        "2025_Q1": "01mar2025-31may2025_form13f.zip",
        "2024_Q3": "01sep2024-30nov2024_form13f.zip",
        "2024_Q2": "01jun2024-31aug2024_form13f.zip",
        "2024_Q1": "01jan2024-29feb2024_form13f.zip",
        "2023_Q4": "2023q4_form13f.zip",
        "2023_Q3": "2023q3_form13f.zip",
        "2023_Q1": "2023q1_form13f.zip",
    }

    BASE_URL = "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/"

    def __init__(
        self,
        quarters: Optional[List[str]] = None,
        output_dir: str = "13f_outputs",
    ):
        """
        Args:
            quarters: List of quarters to download (e.g., ["2025_Q2", "2025_Q1"]). If None, downloads all available quarters.
            output_dir: Output directory for downloads and parsed data.
        """
        self.quarters = quarters or list(self.QUARTERLY_DATASETS.keys())
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def _download_zip(self, url: str, output_path: str) -> None:
        """Downloads SEC quarterly ZIP with proper headers."""
        headers = {"User-Agent": "AsafZenou-Research/1.0"}
        print(f"  Downloading from: {url}")

        try:
            response = requests.get(url, headers=headers, stream=True, timeout=60)
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))
            downloaded = 0

            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size:
                        pct = (downloaded / total_size) * 100


            print(f"  ✓ Downloaded: {output_path}")
        except Exception as e:
            print(f"  ✗ Download failed: {e}")
            raise

    def _extract_zip(self, zip_path: str, extract_to: str) -> None:
        """Extracts ZIP to target directory."""
        os.makedirs(extract_to, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(extract_to)
        print(f"  ✓ Extracted to: {extract_to}")

    def _parse_tsv_file(self, tsv_file: str) -> List[Dict[str, Any]]:
        """Parses single TSV file and returns list of holdings."""
        try:
            rows = []
            with open(tsv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter="\t")
                for row in reader:
                    if row:  # Skip empty rows
                        rows.append(
                            {
                                "cusip": row.get("CUSIP", "").strip(),
                                "nameOfIssuer": row.get("Name of Issuer", "").strip(),
                                "value": row.get("Market Value (x$1000)", "").strip(),
                                "shares": row.get("Shrs or Prin Amt", "").strip(),
                                "share_type": row.get("Sh/Prn", "").strip(),
                                "investment_discretion": row.get(
                                    "Inv. Discretion", ""
                                ).strip(),
                                "put_call": row.get("Put/Call", "").strip(),
                                "voting_sole": row.get("Sole Voting", "").strip(),
                                "voting_shared": row.get("Shared Voting", "").strip(),
                                "voting_none": row.get("No Voting", "").strip(),
                                "tsv_source": tsv_file,
                            }
                        )
            return rows
        except Exception as e:
            print(f"  ⚠ Error parsing {tsv_file}: {e}")
            return []

    def _parse_quarter_folder(self, folder: str, quarter: str) -> List[Dict[str, Any]]:
        """Parses all TSV files in quarter folder recursively."""
        all_rows = []
        tsv_files = glob.glob(f"{folder}/**/*.tsv", recursive=True)
        print(f"  Found {len(tsv_files)} TSV files in {quarter}")

        for i, tsv_file in enumerate(tsv_files, 1):
            if i % 10 == 0:
                print(f"    Parsed {i}/{len(tsv_files)} files...")
            rows = self._parse_tsv_file(tsv_file)
            all_rows.extend(rows)

        print(f"  ✓ {quarter}: {len(all_rows)} holdings parsed")
        return all_rows

    def extract(self) -> List[Dict[str, Any]]:
        """
        Main extraction flow:
        1. Download all requested quarterly ZIPs
        2. Extract each to temp directory
        3. Parse all TSV files per quarter
        4. Combine all records across quarters
        5. Return combined dataset
        """
        temp_dir = tempfile.mkdtemp(prefix="sec_13f_quarterly_")
        all_combined_rows = []

        try:
            print(f"Extracting {len(self.quarters)} quarters...\n")

            for quarter in self.quarters:
                if quarter not in self.QUARTERLY_DATASETS:
                    print(f"⚠ Unknown quarter: {quarter}")
                    continue

                print(f"Processing {quarter}...")
                zip_filename = self.QUARTERLY_DATASETS[quarter]
                zip_url = self.BASE_URL + zip_filename
                zip_path = os.path.join(self.output_dir, zip_filename)

                # Download
                if not os.path.exists(zip_path):
                    self._download_zip(zip_url, zip_path)
                else:
                    print(f"  ✓ Already downloaded: {zip_filename}")

                # Extract
                extract_dir = os.path.join(temp_dir, quarter)
                self._extract_zip(zip_path, extract_dir)

                # Parse
                rows = self._parse_quarter_folder(extract_dir, quarter)

                # Add quarter metadata
                for row in rows:
                    row["quarter"] = quarter

                all_combined_rows.extend(rows)
                print()

            print("=" * 80)
            print(
                f"✓ Total holdings parsed across all quarters: {len(all_combined_rows)}"
            )
            print("=" * 80)

            return all_combined_rows

        finally:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
