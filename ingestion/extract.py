from datetime import datetime, timezone
from pathlib import Path
import logging

import pandas as pd

from ingestion.db import get_engine


# Project configuration
PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "postgres"

# Only allow known tables to avoid unsafe SQL identifiers
TABLES = [
    "customers",
    "suppliers",
    "warehouses",
    "products",
    "orders",
    "order_items",
    "inventory",
]

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def extract_table(table_name: str) -> Path:
    """
    Extract one PostgreSQL table into a Parquet file.
    """

    if table_name not in TABLES:
        raise ValueError(f"Table not allowed: {table_name}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    engine = get_engine()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    output_file = OUTPUT_DIR / f"{table_name}_{timestamp}.parquet"

    query = f"SELECT * FROM {table_name}"

    try:
        logger.info("Starting extraction: %s", table_name)

        with engine.connect() as connection:
            dataframe = pd.read_sql_query(
                query,
                connection,
            )

        dataframe.to_parquet(
            output_file,
            index=False,
            engine="pyarrow",
        )

        logger.info(
            "Extracted %s rows from %s",
            len(dataframe),
            table_name,
        )

        logger.info("Saved file: %s", output_file)

        return output_file

    except Exception:
        logger.exception(
            "Extraction failed for table: %s",
            table_name,
        )
        raise

    finally:
        engine.dispose()


def extract_all_tables():
    """
    Extract all configured PostgreSQL tables.
    """

    results = []

    for table_name in TABLES:
        output_file = extract_table(table_name)
        results.append(output_file)

    logger.info(
        "Extraction completed. Files created: %s",
        len(results),
    )

    return results


if __name__ == "__main__":
    extract_all_tables()
