# The repository for data engineering project for group 7 on the topic of Health of bee populations in US countys.


## Team members
* Remi Raugme
* Annaliisa Vask
* Selene Margaret Pruuden
* Victoria Prins

## Running the project
### Run the project (run these in order)

1. Build images
```bash
docker compose build
```

2. Start services in detached mode
```bash
docker compose up -d
```

3. Create ClickHouse database and tables
```bash
docker exec -it clickhouse-server-project \
    clickhouse-client --multiquery --queries-file=/sql/create_db_and_tables.sql
```

4. Enter the Airflow scheduler container and install dbt dependencies
```bash
docker exec -it airflow-scheduler-project bash
# then, inside the container:
cd /dbt
dbt deps
```
5. Go to http://localhost:8080 log in. The user is airflow and password is airflow. Enable all the DAGs.

6. Move the APHIS and GBIF files into their respective folders and everything should work.


## Visuals from Airflow
### The DAGs used in the project
![Airflow DAGs](visuals/airflow_dags.png)
### Aphis DAG
![Aphis DAG](visuals/airflow_aphis_dag.png)
### GBIF DAG
![GBIF DAG](visuals/airflow_gbif_dag.png)
### dbt DAG
![dbt DAG](visuals/airflow_dbt_dag.png)


## Known issues

### Duplicate insegtion
Sadly we couldn't get non duplicate ingestion working properly. Found a [webpage](https://cc.davelozinski.com/sql/fastest-way-to-insert-new-records-where-one-doesnt-already-exist)
that outlined 4 methods to achieve it and tried all of them excpet merge. They either didn't work on first ingestion or didn't work after the first ingestion. Either still adding everything or adding nothing.