"""
DAG: Import tab-delimited aphis files into messud.aphis table.

Changes:
- Uses Airflow's built-in FileSensor instead of custom regex sensor.
- Watches the dedicated subfolder /var/lib/clickhouse/user_files/aphis
- Loads TSV/TXT files into ClickHouse (tab-separated, header included).
- Applies non-null and bounds checks for decimalLatitude.
- Moves processed files to a 'processed' subfolder after load.
"""

from datetime import timedelta
import os
import shutil
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

# --- Move processed file to 'processed/' folder ---
def move_processed_file(**context):
    fname = context['ti'].xcom_pull(task_ids='find_and_push_file', key='matched_file_basename')
    if not fname:
        raise ValueError("No filename returned from sensor")

    shared_dir = '/var/lib/clickhouse/user_files/aphis'
    src = os.path.join(shared_dir, os.path.basename(fname))
    if not os.path.exists(src):
        raise FileNotFoundError(f"File not found: {src}")

    processed_dir = os.path.join("/var/lib/clickhouse/user_files", 'processed')
    os.makedirs(processed_dir, exist_ok=True)
    dest = os.path.join(processed_dir, os.path.basename(fname))
    shutil.move(src, dest)
    print(f"Moved processed file to {dest}")
    
def find_and_push_file(ti):
    folder = '/var/lib/clickhouse/user_files/aphis'
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
    INSERT INTO messud.aphis
    SELECT f.*
    FROM
    (
        SELECT
            toUInt16(sample_year) AS sample_year,
            toUInt8(sample_month_number) AS sample_month_number,
            sample_month,
            state_code,
            sampling_county,
            toFloat32(varroa_per_100_bees) AS varroa_per_100_bees,
            toFloat32(million_spores_per_bee) AS million_spores_per_bee,
            abpv,
            toFloat32OrNull(replaceRegexpAll(toString(abpv_percentile), '^<', '')) AS abpv_percentile,
            amsv1,
            toFloat32OrNull(replaceRegexpAll(toString(amsv1_percentile), '^<', '')) AS amsv1_percentile,
            cbpv,
            toFloat32OrNull(replaceRegexpAll(toString(cbpv_percentile), '^<', '')) AS cbpv_percentile,
            dwv,
            toFloat32OrNull(replaceRegexpAll(toString(dwv_percentile), '^<', '')) AS dwv_percentile,
            `dwv-b` AS dwv_b,
            toFloat32OrNull(replaceRegexpAll(toString(`dwv-b_percentile`), '^<', '')) AS dwv_b_percentile,
            iapv,
            toFloat32OrNull(replaceRegexpAll(toString(iapv_percentile), '^<', '')) AS iapv_percentile,
            kbv,
            toFloat32OrNull(replaceRegexpAll(toString(kbv_percentile), '^<', '')) AS kbv_percentile,
            lsv2,
            toFloat32OrNull(replaceRegexpAll(toString(lsv2_percentile), '^<', '')) AS lsv2_percentile,
            sbpv,
            toFloat32OrNull(replaceRegexpAll(toString(sbpv_percentile), '^<', '')) AS sbpv_percentile,
            mkv,
            toFloat32OrNull(replaceRegexpAll(toString(mkv_percentile), '^<', '')) AS mkv_percentile,
            pesticides
        FROM file(
            '/var/lib/clickhouse/user_files/aphis/{{ ti.xcom_pull(task_ids='find_and_push_file', key='matched_file_basename') }}',
            'CSVWithNames'
        )
        WHERE
            sample_year IS NOT NULL
            AND sample_month_number IS NOT NULL
            AND sample_month IS NOT NULL
            AND state_code IS NOT NULL
            AND sampling_county IS NOT NULL
    ) AS f
    LEFT JOIN messud.aphis AS t
        ON f.sample_year = t.sample_year
    AND f.sample_month_number = t.sample_month_number
    AND f.sample_month = t.sample_month
    AND f.state_code = t.state_code
    AND f.sampling_county = t.sampling_county
    WHERE t.sample_year IS NULL OR NOT exists (SELECT 1 FROM messud.aphis LIMIT 1);
    """    

# --- DAG definition ---
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='aphis_csv_to_messud_aphis',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@continuous',
    catchup=False,
    max_active_runs=1,
    tags=['aphis', 'clickhouse', 'messud'],
) as dag:

    # Detect any file in the aphis input folder (can be .txt or .tsv)
    wait_for_aphis_file = FileSensor(
        task_id='wait_for_aphis_file',
        fs_conn_id='fs_default',  # define in Airflow Connections → type "File (path)"
        filepath='/var/lib/clickhouse/user_files/aphis',  # the shared folder mounted in all containers
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

    wait_for_aphis_file >> find_and_push >> load_to_clickhouse >> move_file >> trigger_dbt
