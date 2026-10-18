"""
etl_pipeline.py
---------------
Airflow DAG that loads customer, product, and order data from CSV, JSON,
and YAML files into PostgreSQL.

Task flow:
  file_validation >> database_setup >> [extract_csv, extract_json, extract_yaml]
  >> load_customers >> load_products >> load_orders >> data_validation >> completion

How data moves between tasks:
  Extract tasks read each file with pandas, check it, and save it to a
  "staging" folder (data/staging/). Load tasks read from staging, merge the
  three formats, remove duplicates, and bulk-insert into PostgreSQL.
  Only small numbers (row counts) are passed between tasks through XCom.
"""

import logging
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pendulum
import psycopg2
import yaml
from psycopg2.extras import execute_values

# Airflow 3 import paths, with a fallback for Airflow 2.x
try:
    from airflow.sdk import DAG
except ImportError:
    from airflow import DAG
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DAG_NAME = "etl_pipeline"

DATA_DIR = Path(os.environ.get("ETL_DATA_DIR", "/opt/airflow/data"))
SQL_FILE = Path(os.environ.get("ETL_SQL_FILE", "/opt/airflow/sql/schema.sql"))
STAGING_DIR = DATA_DIR / "staging"
ARCHIVE_DIR = DATA_DIR / "archive"

ENTITIES = ["customers", "products", "orders"]
FORMATS = ["csv", "json", "yaml"]

# Columns each entity must have, in database column order
COLUMNS = {
    "customers": ["customer_id", "first_name", "last_name", "email", "phone",
                  "city", "registration_date", "customer_type"],
    "products": ["product_id", "product_name", "category", "brand",
                 "unit_price", "stock_quantity", "is_active"],
    "orders": ["order_id", "customer_id", "order_date", "order_status",
               "payment_method", "total_amount", "discount_amount", "shipping_cost"],
}
PRIMARY_KEYS = {"customers": "customer_id", "products": "product_id", "orders": "order_id"}

log = logging.getLogger(__name__)   # Airflow adds a timestamp to every log line


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_connection():
    """
    Open a PostgreSQL connection. Credentials come from environment variables
    (set from the .env file through docker-compose), never from the code.
    """
    try:
        return psycopg2.connect(
            host=os.environ.get("ETL_DB_HOST", "etl-postgres"),
            port=int(os.environ.get("ETL_DB_PORT", "5432")),
            dbname=os.environ["ETL_DB_NAME"],
            user=os.environ["ETL_DB_USER"],
            password=os.environ["ETL_DB_PASSWORD"],
            connect_timeout=10,
        )
    except KeyError as e:
        raise RuntimeError(f"Missing environment variable {e}. Check .env and docker-compose.yaml") from e
    except psycopg2.OperationalError as e:
        log.error("Could not connect to the database: %s", e)
        raise


def to_python_rows(df):
    """Convert a DataFrame into tuples of plain Python values (NaN -> None)."""
    rows = []
    for record in df.itertuples(index=False):
        row = []
        for value in record:
            if pd.isna(value):
                row.append(None)
            elif hasattr(value, "item"):        # numpy int/float/bool -> Python type
                row.append(value.item())
            else:
                row.append(value)
        rows.append(tuple(row))
    return rows


def utc_now():
    """Current time in UTC (no timezone attached), to match Airflow's timestamps."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def write_audit(status, start_time, records_processed):
    """Record one pipeline run in the etl_audit table."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO etl_audit (dag_name, start_time, end_time, status, records_processed)
                   VALUES (%s, %s, %s, %s, %s)""",
                (DAG_NAME, start_time, utc_now(), status, records_processed),
            )
        conn.commit()
    finally:
        conn.close()


def run_start_time(context):
    """When this DAG run started (used for the audit table)."""
    dag_run = context.get("dag_run")
    start = getattr(dag_run, "start_date", None)
    return start.astimezone(timezone.utc).replace(tzinfo=None) if start else utc_now()


def on_failure(context):
    """Called by Airflow when any task fails: log it and record FAILED in etl_audit."""
    task_id = context["task_instance"].task_id
    log.error("Pipeline FAILED at task '%s': %s", task_id, context.get("exception"))
    try:
        write_audit("FAILED", run_start_time(context), 0)
    except Exception as e:  # never let the callback itself crash
        log.error("Could not write FAILED row to etl_audit: %s", e)


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------
def file_validation(**context):
    """Check that all 9 data files exist, are not empty, and can be opened."""
    problems = []
    for entity in ENTITIES:
        for fmt in FORMATS:
            path = DATA_DIR / f"{entity}.{fmt}"
            try:
                if not path.exists():
                    problems.append(f"missing: {path.name}")
                elif path.stat().st_size == 0:
                    problems.append(f"empty: {path.name}")
                else:
                    with open(path, "r", encoding="utf-8") as f:
                        f.read(100)
                    log.info("OK  %s (%d bytes)", path.name, path.stat().st_size)
            except OSError as e:
                problems.append(f"unreadable: {path.name} ({e})")

    if problems:
        raise FileNotFoundError("File validation failed: " + "; ".join(problems))
    log.info("All %d files found and readable", len(ENTITIES) * len(FORMATS))


def database_setup(**context):
    """Create tables if they don't exist, then clear old data and staging files."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_FILE.read_text(encoding="utf-8"))
            log.info("Tables created or already exist (ran %s)", SQL_FILE.name)
            # Full refresh: empty the tables so every run loads a clean copy.
            # One TRUNCATE for all three tables handles the foreign key safely.
            cur.execute("TRUNCATE TABLE orders, products, customers;")
            log.info("Cleared customers, products and orders tables")
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        log.error("Database setup failed, transaction rolled back: %s", e)
        raise
    finally:
        conn.close()

    if STAGING_DIR.exists():
        shutil.rmtree(STAGING_DIR)
    STAGING_DIR.mkdir(parents=True)
    log.info("Staging folder cleared: %s", STAGING_DIR)


def read_file(path, fmt):
    """Read one data file into a DataFrame using pandas."""
    if fmt == "csv":
        return pd.read_csv(path)
    if fmt == "json":
        return pd.read_json(path, orient="records", convert_dates=False)
    if fmt == "yaml":
        with open(path, "r", encoding="utf-8") as f:
            return pd.DataFrame(yaml.safe_load(f))
    raise ValueError(f"Unknown format: {fmt}")


def extract(fmt, **context):
    """Read and check the 3 files of one format, then save them to staging."""
    total = 0
    for entity in ENTITIES:
        path = DATA_DIR / f"{entity}.{fmt}"
        try:
            df = read_file(path, fmt)
        except Exception as e:
            log.error("Could not parse %s: %s", path.name, e)
            raise

        missing = set(COLUMNS[entity]) - set(df.columns)
        if missing:
            raise ValueError(f"{path.name} is missing columns: {sorted(missing)}")
        if df[PRIMARY_KEYS[entity]].isna().any():
            raise ValueError(f"{path.name} has rows with no {PRIMARY_KEYS[entity]}")

        df = df[COLUMNS[entity]]
        df.to_pickle(STAGING_DIR / f"{entity}_{fmt}.pkl")
        log.info("Extracted %d rows from %s", len(df), path.name)
        total += len(df)
    return total


def load(entity, **context):
    """
    Merge the CSV, JSON and YAML copies of one entity, remove duplicates,
    and bulk-insert into PostgreSQL in a single transaction.
    """
    frames = [pd.read_pickle(STAGING_DIR / f"{entity}_{fmt}.pkl") for fmt in FORMATS]
    combined = pd.concat(frames, ignore_index=True)
    pk = PRIMARY_KEYS[entity]
    df = combined.drop_duplicates(subset=pk, keep="first")
    log.info("%s: %d rows from 3 formats -> %d unique records",
             entity, len(combined), len(df))

    columns = COLUMNS[entity]
    sql = (f"INSERT INTO {entity} ({', '.join(columns)}) VALUES %s "
           f"ON CONFLICT ({pk}) DO NOTHING")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # execute_values sends rows in batches of 500 (bulk insert)
            execute_values(cur, sql, to_python_rows(df), page_size=500)
        conn.commit()
    except psycopg2.IntegrityError as e:
        conn.rollback()
        log.error("Constraint violation loading %s (duplicate key or foreign key); "
                  "rolled back: %s", entity, e)
        raise
    except psycopg2.Error as e:
        conn.rollback()
        log.error("Database error loading %s; rolled back: %s", entity, e)
        raise
    finally:
        conn.close()

    log.info("Loaded %d rows into %s", len(df), entity)
    return len(df)


def data_validation(**context):
    """Check row counts and that every order points to a real customer."""
    ti = context["ti"]
    expected = {e: ti.xcom_pull(task_ids=f"load_{e}") for e in ENTITIES}

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            counts = {}
            for entity in ENTITIES:
                cur.execute(f"SELECT COUNT(*) FROM {entity}")
                counts[entity] = cur.fetchone()[0]

            cur.execute("""SELECT COUNT(*) FROM orders o
                           LEFT JOIN customers c ON o.customer_id = c.customer_id
                           WHERE c.customer_id IS NULL""")
            orphan_orders = cur.fetchone()[0]
    finally:
        conn.close()

    errors = []
    for entity in ENTITIES:
        log.info("%s: %d rows in database (expected %s)", entity, counts[entity], expected[entity])
        if counts[entity] != expected[entity]:
            errors.append(f"{entity}: {counts[entity]} rows, expected {expected[entity]}")
    log.info("Orders without a valid customer: %d", orphan_orders)
    if orphan_orders:
        errors.append(f"{orphan_orders} orders reference missing customers")

    if errors:
        raise ValueError("Data validation failed: " + "; ".join(errors))
    log.info("Data validation passed")
    return sum(counts.values())


def completion(**context):
    """Archive a copy of the processed files and record SUCCESS in etl_audit."""
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive = ARCHIVE_DIR / stamp
    archive.mkdir(parents=True, exist_ok=True)
    for entity in ENTITIES:
        for fmt in FORMATS:
            # Copy (not move) so the source files are still there for the next daily run
            shutil.copy2(DATA_DIR / f"{entity}.{fmt}", archive)
    log.info("Archived 9 files to %s", archive)

    total = context["ti"].xcom_pull(task_ids="data_validation")
    write_audit("SUCCESS", run_start_time(context), total)
    log.info("Pipeline completed successfully: %d records processed", total)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "ahmed",
    "retries": 1,                          # retry a failed task once
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": on_failure,     # record FAILED in etl_audit
}

with DAG(
    dag_id=DAG_NAME,
    description="Load customers, products and orders from CSV/JSON/YAML into PostgreSQL",
    schedule="0 6 * * *",                  # every day at 06:00 UTC
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,                         # don't run for past dates
    default_args=default_args,
    tags=["etl", "data430"],
) as dag:

    t_file_validation = PythonOperator(task_id="file_validation", python_callable=file_validation)
    t_database_setup = PythonOperator(task_id="database_setup", python_callable=database_setup)

    t_extract_csv = PythonOperator(task_id="extract_csv", python_callable=extract, op_kwargs={"fmt": "csv"})
    t_extract_json = PythonOperator(task_id="extract_json", python_callable=extract, op_kwargs={"fmt": "json"})
    t_extract_yaml = PythonOperator(task_id="extract_yaml", python_callable=extract, op_kwargs={"fmt": "yaml"})

    t_load_customers = PythonOperator(task_id="load_customers", python_callable=load, op_kwargs={"entity": "customers"})
    t_load_products = PythonOperator(task_id="load_products", python_callable=load, op_kwargs={"entity": "products"})
    t_load_orders = PythonOperator(task_id="load_orders", python_callable=load, op_kwargs={"entity": "orders"})

    t_data_validation = PythonOperator(task_id="data_validation", python_callable=data_validation)
    t_completion = PythonOperator(task_id="completion", python_callable=completion)

    (
        t_file_validation
        >> t_database_setup
        >> [t_extract_csv, t_extract_json, t_extract_yaml]
        >> t_load_customers
        >> t_load_products
        >> t_load_orders
        >> t_data_validation
        >> t_completion
    )
