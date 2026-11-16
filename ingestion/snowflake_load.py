import logging
from pathlib import Path

from ingestion.snowflake_db import get_snowflake_connection

PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_DIR = PROJECT_ROOT / "data" / "raw" / "postgres"

DATABASE = "ECOMMERCE_DB"
SCHEMA = "BRONZE"
STAGE = "PG_PARQUET_STAGE"
FILE_FORMAT = "PARQUET_FORMAT"

TABLES = [
    "customers",
    "suppliers",
    "warehouses",
    "products",
    "orders",
    "order_items",
    "inventory",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def get_latest_parquet_file(table_name: str) -> Path:
    """Find the latest Parquet extraction for a table."""

    candidates = list(INPUT_DIR.glob(f"{table_name}_*.parquet"))

    if not candidates:
        raise FileNotFoundError(f"No Parquet file found for table: {table_name}")

    return max(candidates, key=lambda path: path.stat().st_mtime)


def upload_parquet(cursor, table_name: str, file_path: Path):
    """Upload a local Parquet file to its stage directory."""

    local_file = file_path.resolve().as_posix()
    local_file_uri = f"file://{local_file}"

    stage_location = f"@{DATABASE}.{SCHEMA}.{STAGE}/{table_name}/"

    sql = f"PUT {local_file_uri} {stage_location} " "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

    logger.info("Uploading %s", file_path.name)

    cursor.execute(sql)

    results = cursor.fetchall()

    for result in results:
        logger.info("Upload result: %s", result)


def create_table_from_parquet(cursor, table_name: str):
    """Infer the Snowflake table schema from staged Parquet."""

    stage_location = f"@{DATABASE}.{SCHEMA}.{STAGE}/{table_name}/"

    table_location = f"{DATABASE}.{SCHEMA}.{table_name.upper()}"

    sql = f"""
        CREATE OR REPLACE TABLE {table_location}
        USING TEMPLATE (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                WITHIN GROUP (ORDER BY ORDER_ID)
            FROM TABLE(
                INFER_SCHEMA(
                    LOCATION => '{stage_location}',
                    FILE_FORMAT => '{FILE_FORMAT}'
                )
            )
        )
    """

    logger.info("Creating table: %s", table_location)

    cursor.execute(sql)


def load_parquet_into_table(cursor, table_name: str):
    """Load staged Parquet data into the Bronze table."""

    stage_location = f"@{DATABASE}.{SCHEMA}.{STAGE}/{table_name}/"

    table_location = f"{DATABASE}.{SCHEMA}.{table_name.upper()}"

    sql = f"""
        COPY INTO {table_location}
        FROM {stage_location}
        FILE_FORMAT = (
            FORMAT_NAME = '{FILE_FORMAT}'
        )
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'ABORT_STATEMENT'
        FORCE = TRUE
    """

    logger.info("Loading data into %s", table_location)

    cursor.execute(sql)

    results = cursor.fetchall()

    for result in results:
        logger.info("COPY result: %s", result)


def verify_table(cursor, table_name: str):
    """Verify the number of rows loaded into a table."""

    table_location = f"{DATABASE}.{SCHEMA}.{table_name.upper()}"

    cursor.execute(f"SELECT COUNT(*) FROM {table_location}")

    row_count = cursor.fetchone()[0]

    logger.info(
        "Verified table %s: %s rows",
        table_name,
        row_count,
    )

    return row_count


def load_all_tables():
    """Run the complete PostgreSQL Parquet to Snowflake load."""

    connection = None
    cursor = None

    try:
        connection = get_snowflake_connection()
        cursor = connection.cursor()

        total_rows = 0

        for table_name in TABLES:
            logger.info("=" * 50)
            logger.info("Processing table: %s", table_name)

            file_path = get_latest_parquet_file(table_name)

            logger.info("Selected file: %s", file_path.name)

            upload_parquet(cursor, table_name, file_path)

            create_table_from_parquet(cursor, table_name)

            load_parquet_into_table(cursor, table_name)

            row_count = verify_table(cursor, table_name)

            total_rows += row_count

        logger.info("=" * 50)
        logger.info("Snowflake ingestion completed!")
        logger.info("Tables loaded: %s", len(TABLES))
        logger.info("Total rows loaded: %s", total_rows)

    except Exception:
        logger.exception("Snowflake ingestion failed")
        raise

    finally:
        if cursor:
            cursor.close()

        if connection:
            connection.close()
            logger.info("Snowflake connection closed")


if __name__ == "__main__":
    load_all_tables()
