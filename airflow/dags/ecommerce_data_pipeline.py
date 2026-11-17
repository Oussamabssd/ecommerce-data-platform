from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator


# --------------------------------------------------
# Configuration
# --------------------------------------------------

PROJECT_DIR = Path("/opt/airflow/project")

DATA_DIR = PROJECT_DIR / "data" / "raw" / "postgres"

TABLES = [
    "customers",
    "suppliers",
    "warehouses",
    "products",
    "orders",
    "order_items",
    "inventory",
]


# --------------------------------------------------
# Task 1: Extract PostgreSQL data
# --------------------------------------------------


def extract_from_postgres():
    from ingestion.extract import extract_all_tables

    extract_all_tables()


# --------------------------------------------------
# Task 2: Validate Parquet files
# --------------------------------------------------


def validate_parquet():
    import pyarrow.parquet as pq

    total_rows = 0

    for table in TABLES:
        files = sorted(
            DATA_DIR.glob(f"{table}_*.parquet"),
            key=lambda file: file.stat().st_mtime,
            reverse=True,
        )

        if not files:
            raise FileNotFoundError(f"No Parquet file found for {table}")

        latest_file = files[0]

        if latest_file.stat().st_size == 0:
            raise ValueError(f"Empty Parquet file: {latest_file}")

        metadata = pq.read_metadata(latest_file)
        row_count = metadata.num_rows

        if row_count == 0:
            raise ValueError(f"No rows found in {latest_file}")

        total_rows += row_count

        print(f"Validated {table}: " f"{row_count} rows")

    print(f"Total validated rows: {total_rows}")


# --------------------------------------------------
# Task 3: Load data into Snowflake Bronze
# --------------------------------------------------


def load_to_snowflake():
    from ingestion.snowflake_load import load_all_tables

    load_all_tables()


# --------------------------------------------------
# DAG definition
# --------------------------------------------------

with DAG(
    dag_id="ecommerce_data_pipeline",
    description="PostgreSQL to Parquet to Snowflake to dbt",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ecommerce", "data-engineering", "snowflake"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_from_postgres",
        python_callable=extract_from_postgres,
    )

    validate_task = PythonOperator(
        task_id="validate_parquet",
        python_callable=validate_parquet,
    )

    load_task = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    dbt_task = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "dbt build "
            "--project-dir /opt/airflow/project/dbt/ecommerce_analytics "
            "--profiles-dir /opt/airflow/project/airflow/dbt_profiles"
        ),
    )

    # Task dependencies

    extract_task >> validate_task >> load_task >> dbt_task
