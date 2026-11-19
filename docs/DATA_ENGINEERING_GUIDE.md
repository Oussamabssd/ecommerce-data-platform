# 📘 Comprehensive Data Engineering Guide & Architectural Handbook

> **Project:** E-Commerce Modern Data Platform  
> **Author:** Oussama (`oussamabssd`)  
> **Topic:** End-to-End Modern Data Stack (PostgreSQL &rarr; Parquet &rarr; Snowflake &rarr; dbt &rarr; Apache Airflow &rarr; Docker)  
> **Purpose:** Deep-dive learning handbook, architectural reference, and interview prep manual.

---

## 📑 Table of Contents

1. [Executive Summary & Fundamental Paradigms](#1-executive-summary--fundamental-paradigms)
   - [OLTP vs. OLAP Separation](#oltp-vs-olap-separation)
   - [ETL vs. ELT: The Architectural Shift](#etl-vs-elt-the-architectural-shift)
   - [The Medallion Architecture (Bronze &rarr; Silver &rarr; Gold)](#the-medallion-architecture-bronze--silver--gold)
2. [Source Layer: Operational Relational Modeling (PostgreSQL)](#2-source-layer-operational-relational-modeling-postgresql)
   - [Data Schema & Third Normal Form (3NF)](#data-schema--third-normal-form-3nf)
   - [Integrity Constraints & Timezone Handling](#integrity-constraints--timezone-handling)
   - [Containerization & Automated DB Seeding](#containerization--automated-db-seeding)
3. [Ingestion & File Serialization (Python & Parquet)](#3-ingestion--file-serialization-python--parquet)
   - [Why Columnar Parquet over CSV or JSON](#why-columnar-parquet-over-csv-or-json)
   - [Safe Database Extraction with SQLAlchemy & Connection Pooling](#safe-database-extraction-with-sqlalchemy--connection-pooling)
   - [Idempotent Batch Naming & Metadata Validation](#idempotent-batch-naming--metadata-validation)
4. [Data Warehousing & Cloud Storage (Snowflake Bronze Layer)](#4-data-warehousing--cloud-storage-snowflake-bronze-layer)
   - [Snowflake Architectural Decoupling: Compute vs. Storage](#snowflake-architectural-decoupling-compute-vs-storage)
   - [Internal Stages & The `PUT` Command](#internal-stages--the-put-command)
   - [Dynamic Schema Inference (`INFER_SCHEMA`)](#dynamic-schema-inference-infer_schema)
   - [Bulk Ingestion with `COPY INTO`](#bulk-ingestion-with-copy-into)
5. [Transformation & Dimensional Modeling (dbt Core)](#5-transformation--dimensional-modeling-dbt-core)
   - [dbt Architecture, DAG Compilation & Jinja Templating](#dbt-architecture-dag-compilation--jinja-templating)
   - [Silver Layer: Staging Views & Source Validation](#silver-layer-staging-views--source-validation)
   - [Silver Layer: Ephemeral Intermediate Models](#silver-layer-ephemeral-intermediate-models)
   - [Gold Layer: Kimball Dimensional Modeling (Star Schema)](#gold-layer-kimball-dimensional-modeling-star-schema)
   - [Surrogate Key Generation with MD5 Hashes](#surrogate-key-generation-with-md5-hashes)
   - [Automated Data Testing & Quality Assurance](#automated-data-testing--quality-assurance)
6. [Workflow Orchestration (Apache Airflow 3.4.0)](#6-workflow-orchestration-apache-airflow-340)
   - [Orchestration Philosophy: DAGs, Tasks & Idempotency](#orchestration-philosophy-dags-tasks--idempotency)
   - [Airflow Architecture Breakdown in Docker](#airflow-architecture-breakdown-in-docker)
   - [DAG Implementation & Task Dependency Engineering](#dag-implementation--task-dependency-engineering)
   - [Handling Retries, SLAs, and Failure Recovery](#handling-retries-slas-and-failure-recovery)
7. [DevOps, Secrets Management & Infrastructure](#7-devops-secrets-management--infrastructure)
   - [Multi-Container Orchestration (`docker-compose.yml`)](#multi-container-orchestration-docker-composeyml)
   - [Twelve-Factor App Configuration & Secret Isolation](#twelve-factor-app-configuration--secret-isolation)
   - [Git Hygiene & Production-Ready Repository Patterns](#git-hygiene--production-ready-repository-patterns)
8. [Data Engineering Interview & Concept Defense Guide](#8-data-engineering-interview--concept-defense-guide)

---

## 1. Executive Summary & Fundamental Paradigms

### OLTP vs. OLAP Separation

In modern software systems, data workloads fall into two fundamentally distinct categories:

| Dimension | OLTP (Online Transaction Processing) | OLAP (Online Analytical Processing) |
| :--- | :--- | :--- |
| **Primary System** | PostgreSQL, MySQL, Oracle | Snowflake, BigQuery, Databricks, Redshift |
| **Workload Pattern** | High frequency, small writes/reads, row-based | Low frequency, massive scans, aggregations |
| **Data Structure** | Highly normalized (3NF) to eliminate redundancy | Denormalized (Star Schema / Fact-Dimension) |
| **Access Latency** | Milliseconds | Seconds to minutes |
| **Query Pattern** | `SELECT * FROM orders WHERE order_id = 42;` | `SELECT customer_city, SUM(order_total) FROM ... GROUP BY 1;` |
| **Concurrency** | Thousands of concurrent users | Dozens of analysts, dashboards, and ML models |

**Why can't we run BI queries directly on PostgreSQL?**  
Analytical queries involve large table scans, multi-table joins, and extensive aggregations (`SUM`, `AVG`, `GROUP BY`). Running these queries against an OLTP database produces row and table locks, consumes CPU/RAM, and degrades production application responsiveness, risking outages during peak checkout hours.

### ETL vs. ELT: The Architectural Shift

Traditional data engineering relied on **ETL** (Extract &rarr; Transform &rarr; Load):
- Data was extracted from sources.
- An intermediate server (e.g., Informatica, Talend, or heavy Python scripts) performed memory-intensive transformations and cleanups.
- Only the final transformed data was loaded into the warehouse.
- **Flaws of ETL:** Heavy computational bottlenecks on the middleware server, loss of raw data fidelity, and high maintenance costs when business transformation requirements changed.

This project implements the modern **ELT** (Extract &rarr; Load &rarr; Transform) paradigm:
1. **Extract & Load:** Raw data is extracted from PostgreSQL, saved as immutable columnar Parquet files, and loaded directly into Snowflake's **Bronze** layer with zero alteration.
2. **Transform:** High-performance, elastic cloud compute (Snowflake Virtual Warehouses) transforms raw data into Silver and Gold layers using SQL and **dbt Core**.
3. **Benefits of ELT:** Compute scalability on demand, historical replayability (since raw data is preserved intact), and declarative version-controlled SQL models.

### The Medallion Architecture (Bronze &rarr; Silver &rarr; Gold)

Originally popularized by the lakehouse paradigm, the **Medallion Architecture** establishes clear data quality tiers within the analytical warehouse:

```
+------------------------------------------------------------------------------------+
|                                MEDALLION PATTERN                                   |
|                                                                                    |
|   +-------------------+       +--------------------+       +-------------------+   |
|   |   BRONZE LAYER    | ----> |    SILVER LAYER    | ----> |    GOLD LAYER     |   |
|   |    (Raw / Dirty)  |       | (Cleaned/Enriched) |       |  (Curated Marts)  |   |
|   +-------------------+       +--------------------+       +-------------------+   |
|   - Untransformed data        - Standardized types         - Star Schema           |
|   - Inferred schema           - Filtered & deduplicated    - Dimension & Fact      |
|   - Audit history & logs      - Ephemeral intermediate     - Optimized for BI &    |
|   - 1:1 with source tables    - Staging views              - Reporting dashboards  |
+------------------------------------------------------------------------------------+
```

---

## 2. Source Layer: Operational Relational Modeling (PostgreSQL)

### Data Schema & Third Normal Form (3NF)

The operational database simulates an e-commerce platform with 7 core entities modeled in **Third Normal Form (3NF)**:

```
  CUSTOMERS (1) <─────── (N) ORDERS (1) <─────── (N) ORDER_ITEMS
                              │                            │
                              │                            │ (N)
                              ▼                            ▼
  SUPPLIERS (1) <─────── (N) PRODUCTS (1) <─────────────── (1)
                              │
                              │ (1)
                              ▼
                         INVENTORY (N) ───────> (1) WAREHOUSES
```

#### The Relational Tables
1. **`customers`**: Stores buyer identities, contact details (`email`), and registration timestamps.
2. **`suppliers`**: Vendors providing inventory goods.
3. **`warehouses`**: Physical distribution hubs across geographic regions.
4. **`products`**: Item catalog linked to a supplier, with SKU, category, and unit price.
5. **`orders`**: Transaction header tracking customer, status (`pending`, `confirmed`, `shipped`, `delivered`, `cancelled`), and timestamp.
6. **`order_items`**: Line items of an order capturing product ID, quantity purchased, and unit price charged at transaction time.
7. **`inventory`**: Snapshot of on-hand inventory levels for each product within each warehouse.

### Integrity Constraints & Timezone Handling

Production databases enforce consistency via strict SQL constraints:

```sql
-- Modern PostgreSQL auto-incrementing identity (SQL standard over legacy SERIAL)
customer_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY;

-- Enforcing domain validity with CHECK constraints
unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0);
order_status VARCHAR(30) NOT NULL DEFAULT 'pending'
    CHECK (order_status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled'));

-- Referential integrity preventing orphan rows
customer_id BIGINT NOT NULL REFERENCES customers(customer_id) ON DELETE RESTRICT;

-- Timezone awareness
created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
```

> **Engineering Principle: Always use `TIMESTAMPTZ`**  
> Plain `TIMESTAMP` stores dates without timezone offsets, causing silent data corruption when users or servers span across timezones. `TIMESTAMPTZ` standardizes all incoming values into UTC internally.

### Containerization & Automated DB Seeding

To ensure complete environment reproducibility:
- PostgreSQL runs inside Docker using official image `postgres:16`.
- The directory `./postgres/init` is mapped into `/docker-entrypoint-initdb.d`.
- PostgreSQL executes scripts in alphabetical order upon initial volume creation:
  - `01_schema.sql`: Creates DDL tables, indexes, and constraints.
  - `02_seed_data.sql`: Populates sample realistic data (customers, products, orders, order items).
- The container includes a Docker `healthcheck` testing `pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"`.

---

## 3. Ingestion & File Serialization (Python & Parquet)

### Why Columnar Parquet over CSV or JSON

| Feature | CSV | JSON | Apache Parquet |
| :--- | :--- | :--- | :--- |
| **Format** | Plain text (row-oriented) | Semi-structured text | Binary (column-oriented) |
| **Compression Ratio** | Poor (1x) | Poor (1x) | Exceptional (5x to 10x with Snappy/Zstandard) |
| **Schema & Typing** | None (everything is string) | Basic (JSON types) | Strongly typed in file metadata footer |
| **Read Efficiency** | Must parse every line & char | Must parse whole JSON | **Projection pushdown:** only read needed columns |
| **Filter Pushdown** | Read entire file | Read entire file | Reads file metadata (min/max stats per column) |

In data engineering pipelines, extracting transactional tables directly to Parquet creates an **immutable, self-describing audit trail** of extracted data batches.

### Safe Database Extraction with SQLAlchemy & Connection Pooling

The Python ingestion script ([`ingestion/extract.py`](file:///c:/Users/pc/Desktop/projects/ecommerce-data-platform/ingestion/extract.py)) implements key enterprise practices:

1. **Connection Pooling:** Uses `create_engine()` with `pool_pre_ping=True` to automatically detect and discard stale or closed connections.
2. **SQL Injection Prevention:** Validates table names against an allowed whitelist:
   ```python
   TABLES = ["customers", "suppliers", "warehouses", "products", "orders", "order_items", "inventory"]
   if table_name not in TABLES:
       raise ValueError(f"Table not allowed: {table_name}")
   ```
3. **Structured Context Management:** Uses `with engine.connect() as connection:` to guarantee that database connections are released back to the pool even if extraction fails.

### Idempotent Batch Naming & Metadata Validation

Files are named with UTC timestamps:
```python
timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
output_file = OUTPUT_DIR / f"{table_name}_{timestamp}.parquet"
```

The validation layer ([`airflow/dags/ecommerce_data_pipeline.py`](file:///c:/Users/pc/Desktop/projects/ecommerce-data-platform/airflow/dags/ecommerce_data_pipeline.py)) inspects Parquet metadata **without decompressing the entire file**:
```python
import pyarrow.parquet as pq

metadata = pq.read_metadata(latest_file)
row_count = metadata.num_rows

if row_count == 0 or latest_file.stat().st_size == 0:
    raise ValueError(f"Validation failed for {latest_file}")
```
Reading `metadata.num_rows` takes sub-milliseconds because it reads only the file footer rather than scanning megabytes or gigabytes of row data.

---

## 4. Data Warehousing & Cloud Storage (Snowflake Bronze Layer)

### Snowflake Architectural Decoupling: Compute vs. Storage

Snowflake separates:
- **Storage Layer:** Micro-partitions stored encrypted in cloud object storage (AWS S3, GCP Cloud Storage, or Azure Blob). You only pay for stored bytes.
- **Compute Layer:** Independent, stateless Virtual Warehouses (`X-Small` to `4X-Large`). You pay per second of execution time and can resize or auto-suspend them at will.
- **Cloud Services Layer:** Query optimization, security, metadata management, and transaction coordination.

Because compute is decoupled from storage, our Airflow dbt workloads and data ingestion pipelines do not compete with BI analysts or web dashboard queries for resources.

### Internal Stages & The `PUT` Command

Snowflake stages represent storage locations for loading or unloading data:
- **External Stages:** Pointing directly to cloud buckets (`s3://...`, `gs://...`).
- **Internal Stages:** Snowflake-managed file repositories associated with databases or schemas.

In [`ingestion/snowflake_load.py`](file:///c:/Users/pc/Desktop/projects/ecommerce-data-platform/ingestion/snowflake_load.py), the `PUT` SQL command stages local Parquet files into Snowflake's internal stage:
```sql
PUT file:///absolute/path/to/customers_20261119.parquet @ECOMMERCE_DB.BRONZE.PG_PARQUET_STAGE/customers/
AUTO_COMPRESS = FALSE
OVERWRITE = TRUE;
```

### Dynamic Schema Inference (`INFER_SCHEMA`)

Traditional data loading requires engineers to write manual, fragile `CREATE TABLE` statements matching source table data types. If a source column changes from `INT` to `BIGINT`, the load breaks.

Snowflake solves this with `INFER_SCHEMA`:
```sql
CREATE OR REPLACE TABLE ECOMMERCE_DB.BRONZE.CUSTOMERS
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        WITHIN GROUP (ORDER BY ORDER_ID)
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@ECOMMERCE_DB.BRONZE.PG_PARQUET_STAGE/customers/',
            FILE_FORMAT => 'PARQUET_FORMAT'
        )
    )
);
```

#### How it works:
1. `INFER_SCHEMA` inspects the embedded Parquet schema footer inside `@PG_PARQUET_STAGE`.
2. `OBJECT_CONSTRUCT(*)` turns each detected column into a column-definition object.
3. `USING TEMPLATE` dynamically compiles and creates the table with exact column names and matching Snowflake types (`NUMBER`, `TEXT`, `TIMESTAMP_TZ`).

### Bulk Ingestion with `COPY INTO`

Data is loaded into the created Bronze table using the columnar bulk loader:
```sql
COPY INTO ECOMMERCE_DB.BRONZE.CUSTOMERS
FROM @ECOMMERCE_DB.BRONZE.PG_PARQUET_STAGE/customers/
FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'ABORT_STATEMENT'
FORCE = TRUE;
```
- `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`: Avoids column order mismatches between Parquet and Snowflake tables.
- `ON_ERROR = 'ABORT_STATEMENT'`: Guarantees transaction atomicity; if one row is corrupt, the entire load rolls back.
- `FORCE = TRUE`: Ensures idempotent re-running for batch reprocessing.

---

## 5. Transformation & Dimensional Modeling (dbt Core)

### dbt Architecture, DAG Compilation & Jinja Templating

**dbt (data build tool)** enables data engineers to write modular SQL `SELECT` statements with software engineering best practices:
- **Dependency Management:** Instead of hardcoding table names like `FROM ECOMMERCE_DB.BRONZE.CUSTOMERS`, dbt models use `{{ source('bronze', 'customers') }}` and `{{ ref('stg_customers') }}`.
- **DAG Compilation:** dbt parses the references to construct a Directed Acyclic Graph (DAG) ensuring upstream staging models execute before intermediate and mart models.
- **Custom Schema Isolation:** The macro [`dbt/ecommerce_analytics/macros/generate_schema_name.sql`](file:///c:/Users/pc/Desktop/projects/ecommerce-data-platform/dbt/ecommerce_analytics/macros/generate_schema_name.sql) overrides dbt's default naming behavior to cleanly route models into `SILVER` and `GOLD` schemas.

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

### Silver Layer: Staging Views & Source Validation

Staging models (`models/staging/stg_*.sql`):
- Materialized as **`view`** (`+materialized: view` in `dbt_project.yml`) because they are lightweight abstraction layers that should never duplicate storage.
- Standardize column names (e.g. converting `CUST_ID` to `customer_id`).
- Standardize data types and timestamps.
- Defined in `sources.yml` against the Bronze schema.

Example from [`stg_orders.sql`](file:///c:/Users/pc/Desktop/projects/ecommerce-data-platform/dbt/ecommerce_analytics/models/staging/stg_orders.sql):
```sql
WITH source AS (
    SELECT * FROM {{ source('bronze', 'orders') }}
),
renamed AS (
    SELECT
        order_id::BIGINT AS order_id,
        order_number::VARCHAR(50) AS order_number,
        customer_id::BIGINT AS customer_id,
        order_status::VARCHAR(30) AS order_status,
        order_date::TIMESTAMPTZ AS order_date,
        updated_at::TIMESTAMPTZ AS updated_at
    FROM source
)
SELECT * FROM renamed
```

### Silver Layer: Ephemeral Intermediate Models

Intermediate models (`models/intermediate/int_*.sql`):
- Configured as **`ephemeral`** (`+materialized: ephemeral`).
- **What is ephemeral?** dbt compiles the model as a Common Table Expression (CTE) directly into the downstream SQL query rather than creating a table or view in Snowflake.
- This provides modular code reuse without polluting the database catalog or incurring DDL overhead.

Example from [`int_orders_enriched.sql`](file:///c:/Users/pc/Desktop/projects/ecommerce-data-platform/dbt/ecommerce_analytics/models/intermediate/int_orders_enriched.sql):
- Aggregates order metrics from `int_order_items_enriched` (total quantity, item counts, order total).
- Joins with customer attributes to compute order-level summaries.

### Gold Layer: Kimball Dimensional Modeling (Star Schema)

The **Gold Layer** structures data according to Ralph Kimball's dimensional modeling technique for fast slice-and-dice business queries:

```
               +----------------------+
               |     dim_products     |
               |----------------------|
               | product_key (PK)     |
               | product_id           |
               | sku, category, price |
               +----------------------+
                         ▲
                         │
+---------------------+  │  +----------------------+
|    dim_customers    |  │  |    dim_warehouses    |
|---------------------|  │  |----------------------|
| customer_key (PK)   |  │  | warehouse_key (PK)   |
| customer_id         |  │  | warehouse_id         |
| full_name, email    |  │  | city, country        |
+---------------------+  │  +----------------------+
          ▲              │              ▲
          │              │              │
    +-----+--------------+--------------+-----+
    │                                         │
+-------------------+                     +--------------------+
|    fct_orders     |                     |   fct_inventory    |
|-------------------|                     |--------------------|
| order_id (PK)     |                     | inventory_id (PK)  |
| customer_id (FK)  |                     | product_id (FK)    |
| customer_key (FK) |                     | warehouse_id (FK)  |
| order_total       |                     | quantity_on_hand   |
| total_quantity    |                     | inventory_value    |
+-------------------+                     +--------------------+
```

#### Fact vs. Dimension Differences:
- **Dimensions (`dim_*`):** The "who, what, where" context. Contain descriptive text attributes (customer name, country, category).
- **Facts (`fct_*`):** The quantitative business measurements and events (sales revenue, items sold, stock counts). Typically contain numerical metrics and foreign keys referencing dimensions.

### Surrogate Key Generation with MD5 Hashes

In dimensional modeling, relying on source system natural keys (`customer_id = 101`) can create risks if:
1. Multiple source systems share overlapping IDs (e.g., merging Shopify and Magento).
2. Slowly Changing Dimensions (SCD Type 2) require tracking multiple versions of a customer over time.

We generate deterministic **surrogate keys** using MD5 hashing in Snowflake:
```sql
SELECT
    MD5(TO_VARCHAR(customer_id)) AS customer_key,
    customer_id,
    full_name,
    email,
    city,
    country,
    created_at
FROM {{ ref('stg_customers') }}
```
- Produces a uniform 32-character hexadecimal key.
- Deterministic: The same input always produces the exact same key without needing an expensive distributed auto-increment counter.

### Automated Data Testing & Quality Assurance

dbt tests compile into SQL assertions that fail if any violating rows are returned:

```yaml
# dbt/ecommerce_analytics/models/marts/schema.yml
models:
  - name: fct_orders
    columns:
      - name: order_id
        data_tests:
          - unique
          - not_null
      - name: customer_id
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_id
```

- **`unique`:** Ensures no duplicate primary keys exist.
- **`not_null`:** Guards against missing foreign keys and IDs.
- **`relationships`:** Verifies referential integrity, ensuring that every order references an actual customer existing in `dim_customers`.

---

## 6. Workflow Orchestration (Apache Airflow 3.4.0)

### Orchestration Philosophy: DAGs, Tasks & Idempotency

A data pipeline must be:
1. **Idempotent:** Running the pipeline twice on the same day with the same input produces the exact same state without duplicating rows or corrupting metrics.
2. **Deterministic:** Dependencies between extraction, staging, loading, and transformations must be strictly enforced.
3. **Resilient:** Transient failures (e.g. temporary network blip) should automatically retry with exponential backoff before notifying engineers.

### Airflow Architecture Breakdown in Docker

The project runs modern **Apache Airflow 3.4.0** configured with a multi-container microservice setup:

```
[Host / Docker Network]
   │
   ├── [airflow-db]             Postgres 16 housing Airflow metadata
   ├── [airflow-init]           Runs migrations (`db migrate`) and exits
   ├── [airflow-scheduler]      Monitors DAGs, schedules runs & manages workers
   ├── [airflow-dag-processor]  Continuously parses DAG python files
   ├── [airflow-triggerer]      Asynchronous event triggers
   └── [airflow-apiserver]      Modern Airflow 3 API Server & Web UI (Port 8080)
```

- **Isolated Metadata DB (`airflow-db`):** Airflow stores task runs, DAG definitions, and connection states in a dedicated PostgreSQL database separate from the operational e-commerce database.
- **`LocalExecutor`:** Executes tasks in parallel worker processes directly inside the container network without needing external Celery or RabbitMQ brokers.

### DAG Implementation & Task Dependency Engineering

In [`airflow/dags/ecommerce_data_pipeline.py`](file:///c:/Users/pc/Desktop/projects/ecommerce-data-platform/airflow/dags/ecommerce_data_pipeline.py):

```python
with DAG(
    dag_id="ecommerce_data_pipeline",
    description="PostgreSQL to Parquet to Snowflake to dbt",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 2 * * *",        # Runs daily at 02:00 UTC
    catchup=False,               # Do not backfill historic missing runs on startup
    max_active_runs=1,           # Prevent overlapping concurrent pipeline runs
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

    # Clean linear task dependency execution
    extract_task >> validate_task >> load_task >> dbt_task
```

### Handling Retries, SLAs, and Failure Recovery

- `retries: 2`: If Snowflake or PostgreSQL encounters a transient connection error, Airflow will automatically retry up to two times.
- `retry_delay: timedelta(minutes=5)`: Enforces a 5-minute cool-down period before attempting a retry.
- `max_active_runs: 1`: Critical data engineering guardrail preventing race conditions where two simultaneous pipeline executions try to write to the same Snowflake Bronze table simultaneously.

---

## 7. DevOps, Secrets Management & Infrastructure

### Multi-Container Orchestration (`docker-compose.yml`)

The platform connects two separate database instances and four Airflow processes on a shared Docker bridge network:
1. `ecommerce_postgres`: Port `5433` on host &rarr; `5432` in container.
2. `ecommerce_airflow_db`: Internal database for Airflow state.
3. Healthcheck dependencies: `airflow-init` waits for `airflow-db` to become healthy; the scheduler and UI wait for `airflow-init` to complete successfully.

### Twelve-Factor App Configuration & Secret Isolation

The project strictly follows the **Twelve-Factor App methodology**:
- **Factor III: Config stored in the environment.**
- No API keys, passwords, or hostnames are hardcoded in application logic.
- Every script reads configuration from environment variables via `python-dotenv` or system environment.
- The repository tracks only [`.env.example`](file:///c:/Users/pc/Desktop/projects/ecommerce-data-platform/.env.example); actual `.env` credentials are strictly ignored in `.gitignore`.

### Git Hygiene & Production-Ready Repository Patterns

- `.gitignore` prevents virtual environments (`.venv/`), runtime logs (`airflow/logs/`, `dbt/logs/`), compilation artifacts (`target/`, `partial_parse.msgpack`), and raw datasets from ever entering git history.
- Empty required directories are preserved across git clones using `.gitkeep`.
- Commit history follows conventional commits (`feat: ...`, `chore: ...`, `docs: ...`).

---

## 8. Data Engineering Interview & Concept Defense Guide

Use these real-world questions and model answers to master interviews discussing this project:

### Q1: Why did you choose ELT over ETL for this platform?
> **Answer:**  
> "In traditional ETL, data is transformed by an intermediate processing engine before landing in the warehouse. This creates compute bottlenecks, adds another point of failure, and discards raw data.  
> In our platform, we adopted ELT: we extract operational records into raw Parquet files and load them directly into Snowflake's Bronze layer. We then utilize Snowflake's scalable, decoupled virtual warehouses along with dbt Core to perform in-warehouse SQL transformations. This gives us full auditability, historical replayability, and enables version-controlled, testable modeling."

### Q2: Why did you extract data into Parquet files before loading it to Snowflake rather than querying Postgres directly?
> **Answer:**  
> "Extracting to Parquet provides four critical architectural benefits:
> 1. **Decoupling:** Extraction runs as an isolated step. If Snowflake is temporarily unreachable, the extracted snapshot is safely preserved on disk or object storage.
> 2. **Columnar Efficiency:** Parquet achieves high compression and encapsulates exact data types and metadata statistics (like row count and min/max values) directly in the file footer.
> 3. **Bulk Ingestion Performance:** Snowflake's `COPY INTO` command is heavily optimized for parallel ingestion from columnar files staged in internal or external stages, drastically outperforming row-by-row JDBC/ODBC inserts.
> 4. **Audit Trail:** Each extract batch is timestamped, providing a point-in-time snapshot of the source database state."

### Q3: How do you handle schema changes in source tables?
> **Answer:**  
> "In the Bronze layer, we leverage Snowflake's `INFER_SCHEMA` and `USING TEMPLATE` features. If the source PostgreSQL table gains new columns, the Parquet file automatically reflects them, and `INFER_SCHEMA` adapts the Bronze table DDL dynamically without manual engineer intervention.  
> In downstream layers, dbt acts as a semantic firewall. Staging views explicitly select and typecast expected columns, preventing unexpected source column additions or renames from silently breaking Gold mart queries."

### Q4: What is the difference between views, ephemeral models, and tables in dbt, and why did you use each?
> **Answer:**  
> - **Views (`stg_*`):** Used in the staging layer. Views avoid duplicating storage for data that merely normalizes column names and types from Bronze tables.
> - **Ephemeral (`int_*`):** Used in intermediate models. Ephemeral models do not create any database object; instead, dbt compiles them as Common Table Expressions (CTEs) within downstream queries, promoting code reusability without catalog clutter.
> - **Tables (`dim_*`, `fct_*`):** Used in the Gold marts layer. Materializing the final Star Schema as physical tables optimizes query performance for business analysts and BI tools by pre-computing joins and aggregations."

### Q5: Why do you generate surrogate keys with MD5 hashes instead of using auto-incrementing integers?
> **Answer:**  
> "Auto-incrementing surrogate keys require a centralized, stateful counter during loading, which introduces distributed concurrency bottlenecks.  
> By using `MD5(TO_VARCHAR(natural_id))`, surrogate key generation becomes completely **deterministic and stateless**. The same natural key always maps to the exact same surrogate hash, whether processed in batch, in parallel, or across different environments, and avoids primary key collision risks when merging multiple source systems."

### Q6: How do you verify data quality in this platform?
> **Answer:**  
> "Quality is enforced at multiple stages:
> 1. **Ingestion stage:** The Airflow `validate_parquet` task checks file existence, non-zero file sizes, and non-zero row counts before allowing staged uploads.
> 2. **Transformation stage:** `dbt test` runs declarative schema tests on every build: `unique` and `not_null` on dimension/fact keys, and `relationships` tests to guarantee referential integrity between fact tables and their dimensions.
> 3. **Snowflake stage:** `ON_ERROR = 'ABORT_STATEMENT'` during `COPY INTO` prevents partially corrupted files from polluting Bronze tables."

---

*This guide represents a complete operational and theoretical blueprint of the E-Commerce Data Platform.*
