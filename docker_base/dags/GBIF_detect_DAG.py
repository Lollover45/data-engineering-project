"""
DAG: Autogenerate ClickHouse table from GBIF file and load it (no copying).
Assumptions:
- Files are placed into host ./sample_data
- ClickHouse container mounts ./sample_data -> /var/lib/clickhouse/user_files
- Airflow containers also mount ./sample_data -> /var/lib/clickhouse/user_files
- A connection clickhouse_default exists pointing to ClickHouse server (plugin).
"""

from datetime import datetime, timedelta
import os
import re
import csv
from typing import List
from airflow import DAG
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.python import PythonOperator
from airflow.utils.decorators import apply_defaults

# plugin imports
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
# Hook path used by the plugin; if your plugin exposes a different import adjust accordingly.
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook


# --- Utility: infer ClickHouse column type from sample values ---
def infer_column_type(samples: List[str]) -> str:
    """
    Heuristic inference:
      - if all ints -> UInt64
      - elif all floats -> Float64
      - elif date-like -> DateTime
      - else -> String
    """
    # remove empty strings for inference
    vals = [v for v in samples if v is not None and v != ""]
    if not vals:
        return "String"

    # int?
    def is_int(s):
        try:
            int(s)
            return True
        except Exception:
            return False

    if all(is_int(v) for v in vals):
        return "UInt64"

    # float?
    def is_float(s):
        try:
            float(s)
            return True
        except Exception:
            return False

    if all(is_float(v) for v in vals):
        return "Float64"

    # date/datetime (common formats)
    date_patterns = [
        r"^\d{4}-\d{2}-\d{2}$",
        r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
        r"^\d{4}/\d{2}/\d{2}$",
        r"^\d{4}_\d{2}_\d{2}_\d{2}_\d{2}$",
        r"^\d{8}$",  # 20251020
    ]
    for p in date_patterns:
        if all(re.match(p, v) for v in vals):
            return "DateTime"

    # fallback
    return "String"


# --- Sensor that detects GBIF filename pattern in the shared ClickHouse user_files folder ---
class PatternFileSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, folder_path: str, filename_regex: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.folder_path = folder_path
        self.filename_re = re.compile(filename_regex)

    def poke(self, context):
        self.log.info("Scanning folder: %s", self.folder_path)
        try:
            for fname in os.listdir(self.folder_path):
                if self.filename_re.match(fname):
                    full = os.path.join(self.folder_path, fname)
                    self.log.info("Found match: %s", full)
                    context['ti'].xcom_push(key='matched_file_basename', value=fname)
                    return True
        except FileNotFoundError:
            self.log.warning("Folder not found: %s", self.folder_path)
        return False


# --- Create table using ClickHouseHook (inferring types from first N rows) ---
def create_table_from_csv(**context):
    """
    Reads the detected CSV's header and first N rows, infers column types,
    and issues CREATE TABLE IF NOT EXISTS <table_name> using ClickHouseHook.
    The table name used here is `gbif_records` (adjustable).
    """
    ti = context['ti']
    fname = ti.xcom_pull(task_ids='wait_for_gbif_file', key='matched_file_basename')
    if not fname:
        raise ValueError("No filename found in XCom")

    # file path inside the container (shared mount)
    shared_dir = '/var/lib/clickhouse/user_files'
    file_path = os.path.join(shared_dir, fname)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} not found (ensure folder is mounted into Airflow container)")

    # read header + sample rows
    with open(file_path, newline='') as f:
        reader = csv.DictReader(f, delimiter='\t')
        headers = reader.fieldnames
        if not headers:
            raise ValueError("CSV has no header; cannot infer schema")

        # sample rows
        sample_rows = []
        max_samples = 50
        for i, row in enumerate(reader):
            if i >= max_samples:
                break
            sample_rows.append(row)

    # create sample values per column
    col_samples = {h: [] for h in headers}
    for r in sample_rows:
        for h in headers:
            col_samples[h].append((r.get(h) if r.get(h) is not None else ""))

    # infer types
    columns_ddl = []
    for h in headers:
        samples = col_samples.get(h, [])
        col_type = infer_column_type(samples)
        # sanitize column names: replace spaces, keep alnum and _
        col_name = re.sub(r'\W+', '_', h).strip('_')
        columns_ddl.append(f"`{col_name}` {col_type}")

    # add metadata column for origin file timestamp (optional)
    columns_ddl.append("`file_name` String")

    table_name = "gbif_records"

    columns_ddl_str = ',\n      '.join(columns_ddl)
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
      {columns_ddl_str}
    ) ENGINE = MergeTree()
    ORDER BY tuple();
    """

    # Use plugin Hook to execute DDL
    self_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
    self_hook.execute(create_sql)
    # push the table name for downstream tasks
    ti.xcom_push(key='created_table', value=table_name)
    ti.xcom_push(key='file_basename', value=fname)
    print(f"Created/verified table {table_name} with columns: {columns_ddl}")


# --- DAG wiring ---
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='gbif_autoschema_clickhouse_load',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@once',
    catchup=False,
    max_active_runs=1,
    tags=['gbif', 'clickhouse', 'autoschema'],
) as dag:

    wait_for_gbif_file = PatternFileSensor(
        task_id='wait_for_gbif_file',
        folder_path='/var/lib/clickhouse/user_files',  # watch the shared folder directly
        filename_regex=r"^GBIF_\d{4}_\d{2}_\d{2}_\d{2}_\d{2}(?:\.csv)?$",
        poke_interval=20,
        timeout=3600,
        mode='reschedule',  # avoid tying up a worker
    )

    create_table = PythonOperator(
        task_id='create_table_from_csv',
        python_callable=create_table_from_csv,
        provide_context=True,
    )

    # Templated SQL for loading: use ClickHouse file() table function to ingest the CSV.
    # We use ti.xcom_pull to get the filename and the created table name.
    load_sql = """
    INSERT INTO {{ ti.xcom_pull(task_ids='create_table_from_csv', key='created_table') }}
    SELECT
      *
    FROM file('/var/lib/clickhouse/user_files/{{ ti.xcom_pull(task_ids='create_table_from_csv', key='file_basename') }}', 'TSVWithNames')
    """

    load_to_clickhouse = ClickHouseOperator(
        task_id='load_to_clickhouse',
        sql=load_sql,
        clickhouse_conn_id='clickhouse_default',
    )

    wait_for_gbif_file >> create_table >> load_to_clickhouse
