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
  python etl_pipeline.py --source sec-zip --quarters 2025_Q2 2025_Q1 --cik 0001067983

"""

import os
import pandas as pd
from extractors.external.sec_extraction_strategy import SECExtractionStrategy
from manipulation.manipulation import DataManipulation
from load.load import DataLoader
from logger.logger import ETLLogger


def main():
    """Main ETL pipeline execution."""

    # Initialize logger
    logger = ETLLogger(name="ETL_Pipeline")

    # ==================== EXTRACT ====================
    logger.info("=" * 80)
    logger.info("STAGE 1: EXTRACTION")
    logger.info("=" * 80)

    try:
        extractor = SECExtractionStrategy(quarters=["2025_Q2"])
        df = extractor.extract()

        logger.info(f"Extraction complete: {len(df)} records")
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        logger.exception("Extraction error details:")
        return 1

    # ==================== MANIPULATION ====================
    logger.info("")
    logger.info("=" * 80)
    logger.info("STAGE 2: MANIPULATION")
    logger.info("=" * 80)

    try:
        manipulator = DataManipulation()

        # Apply all manipulations in sequence
        df = manipulator.manipulate(
            df, operations=["clean", "standardize", "compute", "sort"]
        )

        logger.info(f"manipulation complete: {len(df)} records")
    except Exception as e:
        logger.error(f"manipulation failed: {str(e)}")
        logger.exception("manipulation error details:")
        return 1

    # ==================== LOAD ====================
    logger.info("")
    logger.info("=" * 80)
    logger.info("STAGE 3: LOAD")
    logger.info("=" * 80)

    try:
        loader = DataLoader(output_dir="13f_outputs")

        # Save in multiple formats using main load function
        loader.load(df, "portfolio_13f", formats=["csv", "excel"])
        loader.save_summary_report(df, "portfolio_13f")

        logger.info("load complete: files saved successfully")
    except Exception as e:
        logger.error(f"load failed: {str(e)}")
        logger.exception("load error details:")
        return 1

    # ==================== COMPLETION ====================
    logger.info("")
    logger.info("=" * 80)
    logger.info("✓ ETL PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 80)
    logger.info(f"Log file saved to: {logger.get_log_file()}")

    logger.close()
    return 0


if __name__ == "__main__":
    exit(main())
