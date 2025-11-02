from airflow.datasets import Dataset
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="dbt_run",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['aphis', 'gbif', 'clickhouse', 'messud', 'dbt'],
) as dag:
    dbt_stg_gbif = BashOperator(
        task_id = "dbt_stg_gbif",
        bash_command="cd /dbt && dbt run --select stg_gbif"
    )
    
    dbt_stg_aphis = BashOperator(
        task_id = "dbt_stg_aphis",
        bash_command="cd /dbt && dbt run --select stg_aphis_long"
    )
    
    dbt_dim_date = BashOperator(
        task_id = "dbt_dim_date",
        bash_command="cd /dbt && dbt run --select dim_date"
    )
    
    dbt_dim_location = BashOperator(
        task_id = "dbt_dim_location",
        bash_command="cd /dbt && dbt run --select dim_location"
    )
    
    dbt_dim_organism = BashOperator(
        task_id = "dbt_dim_organism",
        bash_command="cd /dbt && dbt run --select dim_organism"
    )
    
    dbt_fact_observations = BashOperator(
        task_id = "dbt_fact_observations",
        bash_command="cd /dbt && dbt run --select fact_observations"
    )
    
dbt_stg_gbif >> dbt_stg_aphis >> dbt_dim_date >> dbt_dim_location >> dbt_dim_organism >> dbt_fact_observations