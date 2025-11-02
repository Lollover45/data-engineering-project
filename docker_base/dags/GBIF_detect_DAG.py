"""
DAG: Import tab-delimited GBIF files into messud.gbif table.

Changes:
- Uses Airflow's built-in FileSensor instead of custom regex sensor.
- Watches the dedicated subfolder /var/lib/clickhouse/user_files/gbif
- Loads TSV/TXT files into ClickHouse (tab-separated, header included).
- Applies non-null and bounds checks for decimalLatitude.
- Moves processed files to a 'processed' subfolder after load.
"""

from datetime import timedelta
import os
import shutil
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.utils.dates import days_ago

# --- Move processed file to 'processed/' folder ---
def move_processed_file(**context):
    fname = context['ti'].xcom_pull(task_ids='find_and_push_file', key='matched_file_basename')
    if not fname:
        raise ValueError("No filename returned from sensor")

    shared_dir = '/var/lib/clickhouse/user_files/gbif'
    src = os.path.join(shared_dir, os.path.basename(fname))
    if not os.path.exists(src):
        raise FileNotFoundError(f"File not found: {src}")

    processed_dir = os.path.join("/var/lib/clickhouse/user_files", 'processed')
    os.makedirs(processed_dir, exist_ok=True)
    dest = os.path.join(processed_dir, os.path.basename(fname))
    shutil.move(src, dest)
    print(f"Moved processed file to {dest}")
    
def find_and_push_file(ti):
    folder = '/var/lib/clickhouse/user_files/gbif'
    # list files, ignore processed/
    files = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
    if not files:
        raise ValueError("No files found in folder (unexpected, sensor said there was one)")
    # pick the oldest or first (adjust as needed)
    files.sort()
    chosen = files[0]
    ti.xcom_push(key='matched_file_basename', value=chosen)
    print("Pushed matched_file_basename:", chosen)
    
# Load valid rows into ClickHouse (tab-delimited with headers)
load_sql = """
    INSERT INTO messud.gbif
    SELECT f.*
    FROM
    (
        SELECT
            toUInt64(gbifID) AS gbifID, 
            eventDate AS eventDate,
            toUInt16(year) AS year,
            toUInt8(month) AS month,
            toUInt8(day) AS day,
            toUInt64(individualCount) AS individualCount,
            continent,
            countryCode,
            stateProvince,
            county AS county,
            toFloat64(decimalLatitude) AS decimalLatitude,
            toFloat64(decimalLongitude) AS decimalLongitude,
            scientificName AS scientificName,
            species AS species
        FROM file(
            '/var/lib/clickhouse/user_files/gbif/{{ ti.xcom_pull(task_ids='find_and_push_file', key='matched_file_basename') }}',
            'TSVWithNames'
        )
        WHERE
            year IS NOT NULL
            AND month IS NOT NULL
            AND toInt64(individualCount) > 0
            AND countryCode = 'US'
            AND county IS NOT NULL
            AND decimalLatitude IS NOT NULL
            AND toFloat64(decimalLatitude) BETWEEN -90 AND 90
            AND decimalLongitude IS NOT NULL
            AND toFloat64(decimalLongitude) BETWEEN -180 AND 180
            AND species = 'Apis mellifera'
    ) AS f
    LEFT JOIN messud.gbif AS t
        ON f.gbifID = t.gbifID
    WHERE t.gbifID IS NULL OR NOT exists (SELECT 1 FROM messud.gbif LIMIT 1);
    """    

# --- DAG definition ---
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='gbif_tsv_to_messud_gbif',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@continuous',
    catchup=False,
    max_active_runs=1,
    tags=['gbif', 'clickhouse', 'messud'],
) as dag:

    # Detect any file in the GBIF input folder (can be .txt or .tsv)
    wait_for_gbif_file = FileSensor(
        task_id='wait_for_gbif_file',
        fs_conn_id='fs_default',  # define in Airflow Connections → type "File (path)"
        filepath='/var/lib/clickhouse/user_files/gbif',  # the shared folder mounted in all containers
        poke_interval=20,
        timeout=3600,
        mode='reschedule',
        recursive=True,  # scan for new files inside this folder
    )
    
    find_and_push = PythonOperator(
    task_id='find_and_push_file',
    python_callable=find_and_push_file,
    )

    load_to_clickhouse = ClickHouseOperator(
        task_id='load_to_clickhouse',
        sql=load_sql,
        clickhouse_conn_id='clickhouse_default',
        settings={"input_format_tsv_crlf_end_of_line": 1}
    )

    move_file = PythonOperator(
        task_id='move_processed_file',
        python_callable=move_processed_file,
    )
    
    trigger_dbt = TriggerDagRunOperator(
    task_id="trigger_dbt_models",
    trigger_dag_id="dbt_run", 
    wait_for_completion=True,     
    )

    wait_for_gbif_file >> find_and_push >> load_to_clickhouse >> move_file >> trigger_dbt
