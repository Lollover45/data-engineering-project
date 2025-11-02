# The repository for data engineering project for group 7 on the topic of Helth of bee populations in US countys.


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
