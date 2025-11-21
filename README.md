# The repository for the data engineering project for group 7 on the topic of the health of bee populations in US counties.


The previous reports can be found in [P1_report.pdf](P1_report.pdf) for project 1 and [P2_report.pdf](P2_report.pdf) for project 2.

<!-- The report for project 3 is in [P3_report.pdf](P3_report.pdf).) -->

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
5. Go to http://localhost:8080 log in. The user is `airflow` and password is `airflow`. Enable all the DAGs.

6. 
   * Manually create 2 new folders with the names of `aphis` and `gbif` into the following path: `docker_base/sample_data/`. 
   * Then move the file named `APHIS...short.csv` into `aphis` folder and `GBIF...short.txt` into `gbif` folder.

7. The password for clickhouse webUI is `12345678`.


## Visuals from Airflow
### The DAGs used in the project
![Airflow DAGs](visuals/airflow_dags.png)
### Aphis DAG
![Aphis DAG](visuals/airflow_aphis_dag.png)
### GBIF DAG
![GBIF DAG](visuals/airflow_gbif_dag.png)
### dbt DAG
![dbt DAG](visuals/airflow_dbt_dag.png)


## Results for analytical queries
The queries can be found in docker_base/sql/demo_queries.sql
1. From the three  counties with the highest average virus prevalence - how many bees were detected during the year 2024?
 ![top virus](visuals/top3_virus.png)
2. How many bee occurrences are there in the five counties with the fewest Varroa mites?
 ![varroa min](visuals/top5_min_varroa.png)
3. How many bee occurrences are there in the five counties with the most Varroa mites?
 ![varroa max](visuals/top5_max_varroa.png)
4. How many bee occurrences are there in the five counties with the least Nosema fungus?
 ![nosema min](visuals/top5_min_nosema.png)
5. How many bee occurrences are there in the five counties with the most Nosema fungus?
 ![nosema max](visuals/top5_max_nosema.png)
6. Which county is most popular for beekeeping and which is most safe from pests?
 ![pest score](visuals/pest_score.png)
## Known issues

### Duplicate ingestion
Sadly we couldn't get non duplicate ingestion working properly. Found a [webpage](https://cc.davelozinski.com/sql/fastest-way-to-insert-new-records-where-one-doesnt-already-exist)
that outlined 4 methods to achieve it and tried all of them except MERGE. They either didn't work on the first ingestion or didn't work after the first ingestion — either still adding everything or adding nothing.