#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETL pipeline for building a portfolio dataset from 13F filings or local files.

Usage examples:

  # From local CSV
  python etl_pipeline.py --source csv --path data/input.csv

  # From local XML
  python etl_pipeline.py --source xml --path data/input.xml

  # From SEC 13F ZIP dataset
  python etl_pipeline.py --source sec-zip --url "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/01jun2025-31aug2025_form13f.zip"

"""

from Extractors.External.sec_extraction_strategy import SECExtractionStrategy


def main():
    x = SECExtractionStrategy(
        quarters=["2025_Q2", "2024_Q3"]
    )
    x.extract()