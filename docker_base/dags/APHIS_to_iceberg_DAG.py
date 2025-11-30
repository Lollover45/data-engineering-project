"""
DAG: Create or update Iceberg table 'aphis' in REST catalog.

This DAG is triggered by the main ClickHouse load DAG.
It reads the processed file from MinIO and writes an Iceberg table
using PyIceberg.
"""

import os
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from pyiceberg.catalog import load_catalog
from airflow.models.param import Param
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
import pyarrow as pa
import pyarrow.csv as csv
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType, FloatType, LongType

iceberg_fields = [
    NestedField(1, "sample_year", IntegerType(), required=True),  # UInt16
    NestedField(2, "sample_month_number", IntegerType(), required=True),  # UInt8
    NestedField(3, "sample_month", StringType(), required=True),
    NestedField(4, "state_code", StringType(), required=True),
    NestedField(5, "sampling_county", StringType(), required=True),
    NestedField(6, "varroa_per_100_bees", FloatType(), required=False),
    NestedField(7, "million_spores_per_bee", FloatType(), required=False),
    NestedField(8, "abpv", StringType(), required=False),
    NestedField(9, "abpv_percentile", FloatType(), required=False),
    NestedField(10, "amsv1", StringType(), required=False),
    NestedField(11, "amsv1_percentile", FloatType(), required=False),
    NestedField(12, "cbpv", StringType(), required=False),
    NestedField(13, "cbpv_percentile", FloatType(), required=False),
    NestedField(14, "dwv", StringType(), required=False),
    NestedField(15, "dwv_percentile", FloatType(), required=False),
    NestedField(16, "dwv_b", StringType(), required=False),
    NestedField(17, "dwv_b_percentile", FloatType(), required=False),
    NestedField(18, "iapv", StringType(), required=False),
    NestedField(19, "iapv_percentile", FloatType(), required=False),
    NestedField(20, "kbv", StringType(), required=False),
    NestedField(21, "kbv_percentile", FloatType(), required=False),
    NestedField(22, "lsv2", StringType(), required=False),
    NestedField(23, "lsv2_percentile", FloatType(), required=False),
    NestedField(24, "sbpv", StringType(), required=False),
    NestedField(25, "sbpv_percentile", FloatType(), required=False),
    NestedField(26, "mkv", StringType(), required=False),
    NestedField(27, "mkv_percentile", FloatType(), required=False),
    NestedField(28, "pesticides", StringType(), required=False),
]

def load_into_iceberg(**context):

    # find last processed file
    fname = context["dag_run"].conf.get("filename")
    path = os.path.join('/var/lib/clickhouse/user_files/aphis/', fname)
    

    print("Using CSV:", path)

    arrow_table = csv.read_csv(path)
    
    rename_map = {
        "dwv-b": "dwv_b",
        "dwv-b_percentile": "dwv_b_percentile"
    }

    new_names = [rename_map.get(name, name) for name in arrow_table.column_names]
    arrow_table = arrow_table.rename_columns(new_names)

    # 3. Load Iceberg catalog
    catalog = load_catalog(name="rest")
    namespace = "messud_iceberg"
    table_name = "aphis"
    identifier = f"{namespace}.{table_name}"

    # 4. Ensure namespace exists
    try:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass

    # 5. Drop old table (optional)
    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    iceberg_schema = Schema(*iceberg_fields)
    iceberg_schema = iceberg_schema.as_arrow()
    
    # 6. Create a new Iceberg table using Arrow schema
    table = catalog.create_table(
        identifier=identifier,
        schema=iceberg_schema
    )

    arrow_table = arrow_table.cast(iceberg_schema)
    #print(arrow_table)
    # 7. Append the data
    table.append(arrow_table)

    print(f"✅ Iceberg table '{identifier}' written successfully.")


# ---- DAG ----

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="aphis_to_iceberg",
    params={
        "filename": Param(default="", type="string"),
    },
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["iceberg", "aphis"],
) as dag:

    write_iceberg = PythonOperator(
        task_id="write_iceberg_table",
        python_callable=load_into_iceberg
    )
