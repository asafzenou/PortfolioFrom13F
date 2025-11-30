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

  # From SEC 13F (legacy CIK + date range)
  python etl_pipeline.py --source sec --cik 0001067983 --from 2020-01-01 --to 2024-12-31 --identity "Your Name your@email"

"""

import argparse
from typing import List, Dict, Any

# ---- Extract layer ---- #
from Extractors.extractor_context import ExtractorContext
from Extractors.LocalFile.csv_strategy import CSVExtractionStrategy
from Extractors.LocalFile.xml_strategy import XMLExtractionStrategy
from Extractors.External.sec_extraction_strategy import SECExtractionStrategy

# ---- Transform layer ---- #
from Transform.manipulation import transform_records

# ---- Load layer ---- #
from Load.load import Loader


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ETL pipeline for portfolio construction from 13F or local files"
    )

    parser.add_argument(
        "--source",
        required=True,
        choices=["csv", "xml", "sec", "sec-zip"],
        help="Data source type: csv | xml | sec (legacy) | sec-zip (ZIP dataset)",
    )

    # Local file arguments
    parser.add_argument("--path", help="Path to local CSV/XML file")

    # SEC ZIP arguments
    parser.add_argument("--url", help="SEC 13F dataset ZIP URL (for sec-zip mode)")

    # Legacy SEC arguments
    parser.add_argument("--cik", help="Company CIK (for SEC mode)")
    parser.add_argument("--from", dest="date_from", help="Start date YYYY-MM-DD (SEC)")
    parser.add_argument("--to", dest="date_to", help="End date YYYY-MM-DD (SEC)")
    parser.add_argument(
        "--identity",
        help='SEC identity string, e.g. "Your Name your@email" (required for SEC)',
    )

    # Load options
    parser.add_argument(
        "--load-sql",
        action="store_true",
        help="Load results into SQL database (if configured)",
    )
    parser.add_argument(
        "--load-graph",
        action="store_true",
        help="Load results into Graph database (if configured)",
    )

    return parser.parse_args()


def build_extractor(args: argparse.Namespace) -> ExtractorContext:
    """
    Selects the appropriate extraction strategy based on --source
    and returns a ready-to-execute ExtractorContext.
    """
    if args.source == "csv":
        if not args.path:
            raise ValueError("For source=csv you must provide --path")
        strategy = CSVExtractionStrategy(args.path)

    elif args.source == "xml":
        if not args.path:
            raise ValueError("For source=xml you must provide --path")
        strategy = XMLExtractionStrategy(args.path)

    elif args.source == "sec-zip":
        if not args.url:
            raise ValueError("For source=sec-zip you must provide --url")
        strategy = SECExtractionStrategy(sec_zip_url=args.url)

    elif args.source == "sec":
        if not (args.cik and args.date_from and args.date_to and args.identity):
            raise ValueError(
                "For source=sec you must provide --cik, --from, --to, and --identity"
            )
        strategy = SECExtractionStrategy(
            cik=args.cik,
            date_from=args.date_from,
            date_to=args.date_to,
            identity=args.identity,
            output_dir="13f_outputs",
        )
    else:
        raise ValueError(f"Unknown source: {args.source}")

    return ExtractorContext(strategy)


def main() -> int:
    args = parse_args()

    print("=" * 80)
    print("ETL PIPELINE – START")
    print(f"Source type: {args.source}")
    print("=" * 80)

    # 1) EXTRACT
    extractor_ctx = build_extractor(args)
    print("[STEP 1] Extracting raw data...")
    raw_records: List[Dict[str, Any]] = extractor_ctx.execute()
    print(f"[OK] Extracted {len(raw_records)} records")

    if not raw_records:
        print("[WARN] No data extracted. Stopping.")
        return 0

    # 2) TRANSFORM
    print("\n[STEP 2] Transforming data...")
    transformed_records = transform_records(raw_records)
    print(f"[OK] Transformed {len(transformed_records)} records")

    # 3) LOAD
    print("\n[STEP 3] Loading data...")
    loader = Loader()

    if args.load_sql:
        print("  -> Loading into SQL...")
        loader.load_to_sql(transformed_records)

    if args.load_graph:
        print("  -> Loading into Graph DB...")
        loader.load_to_graph(transformed_records)

    if not args.load_sql and not args.load_graph:
        print("  -> No load target selected. Use --load-sql and/or --load-graph.")
        preview = transformed_records[:3]
        print("\nSample transformed rows:")
        for row in preview:
            print(f"  {row}")

    print("\nETL PIPELINE – DONE ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
