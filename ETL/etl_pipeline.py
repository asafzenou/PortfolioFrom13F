import argparse
import logging
import sys
from pathlib import Path

# ============ IMPORT STRATEGIES ============
from Extractors.extractor_context import ExtractorContext
from Extractors.csv_strategy import CSVExtractionStrategy
from Extractors.xml_strategy import XMLExtractionStrategy
from Extractors.db_strategy import DBExtractionStrategy

# ============ TRANSFORM ============
from Transform.manipulation import DataTransformer

# ============ LOAD ============
from Load.load import DataLoader

# ============ DB LAYER ============
from DAL.dal import DAL
from DataHandlers.DBDataHandler.sql_db_handler import SQLDBHandler
from DataHandlers.DBDataHandler.graph_db_handler import GraphDBHandler


# ============================
#      LOGGING CONFIG
# ============================
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


# ============================
#   AUTO DETECT FILE TYPE
# ============================
def select_file_strategy(filepath: str):
    ext = Path(filepath).suffix.lower()

    if ext == ".csv":
        logger.info("Detected CSV file → using CSVExtractionStrategy")
        return CSVExtractionStrategy(filepath)

    elif ext == ".xml":
        logger.info("Detected XML file → using XMLExtractionStrategy")
        return XMLExtractionStrategy(filepath)

    else:
        raise ValueError(f"Unsupported file type: {ext}")


# ============================
#   SELECT DB STRATEGY
# ============================
def select_db_handler(db_type: str):
    if db_type == "sql":
        return SQLDBHandler("example.db")

    elif db_type == "graph":
        return GraphDBHandler(
            uri="bolt://localhost:7687", user="neo4j", password="password"
        )

    else:
        raise ValueError(f"Unsupported DB type: {db_type}")


# ============================
#      PIPELINE EXECUTION
# ============================
def run_pipeline(args):
    logger.info("🚀 Starting ETL pipeline")

    # ============================
    # EXTRACT
    # ============================
    if args.file:
        logger.info(f"Extracting data from file: {args.file}")
        strategy = select_file_strategy(args.file)

    elif args.db_query:
        logger.info(f"Extracting data from database using query:\n{args.db_query}")
        db_handler = select_db_handler(args.db_type)
        dal = DAL(db_handler)

        strategy = DBExtractionStrategy(args.db_query, dal.db_handler)

    else:
        logger.error("No input source provided. Use --file or --db-query")
        sys.exit(1)

    extractor = ExtractorContext(strategy)

    try:
        data = extractor.execute()
        logger.info(f"Extraction complete. Rows extracted: {len(data)}")
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)

    # ============================
    # TRANSFORM
    # ============================
    transformer = DataTransformer()
    try:
        transformed = transformer.transform(data)
        logger.info("Transformation complete.")
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        sys.exit(1)

    # ============================
    # LOAD
    # ============================
    loader = DataLoader()
    try:
        loader.load(transformed)
        logger.info("Load complete.")
    except Exception as e:
        logger.error(f"Load failed: {e}")
        sys.exit(1)

    logger.info("🎉 ETL Pipeline finished successfully!")


# ============================
#          CLI SETUP
# ============================
def main():
    parser = argparse.ArgumentParser(description="ETL Pipeline Runner")

    parser.add_argument("--file", type=str, help="Path to CSV/XML file for extraction")

    parser.add_argument(
        "--db-query", type=str, help="SQL | Cypher query to extract from database"
    )

    parser.add_argument(
        "--db-type", type=str, default="sql", help="Type of DB: sql | graph"
    )

    args = parser.parse_args()
    run_pipeline(args)


if __name__ == "__main__":
    main()
