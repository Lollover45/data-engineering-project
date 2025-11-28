# The repository for the data engineering project for group 7 on the topic of the health of bee populations in US counties.


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

7. The password for clickhouse webUI is 12345678.

### 8 Running Iceberg (it is important to run all the 1.-7. steps beforehand)
### 9 Running Clickhouse roles (it is important to run all the 1.-7. steps beforehand)

### 10 Running OpenMetadata (it is important to run all the 1.-7. steps beforehand)
PS! There might rise an issue that elastic search container won't stay running. If this happens then go in Docker UI Settings -> Resources and increase the Memory Limit value.

1. Create a role that can access database on OpenMetadata
```bash
docker exec -it clickhouse-server-project clickhouse-client
# then, inside the container:
DROP ROLE IF EXISTS role_openmetadata; 
DROP USER IF EXISTS service_openmetadata; 
CREATE ROLE role_openmetadata;
CREATE USER service_openmetadata IDENTIFIED WITH sha256_password BY 'omd_very_secret_password';
GRANT role_openmetadata TO service_openmetadata;
GRANT SELECT, SHOW ON system.* to role_openmetadata;
GRANT SELECT ON messud.* TO role_openmetadata;
```

2. Go to http://localhost:8585 and log in. The username is admin@open-metadata.org and password is admin.

3. In the OpenMetadata UI go to Settings -> Services -> Databases and open clickhouse_server_project_3. 

   If you can't see aforementioned service name then create the connection yourself following the next step on 3.1!

   3.1. Add new service -> Clickhouse
   ```bash
   Service Name: clickhouse_server_project
   Username: service_openmetadata
   Password: omd_very_secret_password
   Host and Port: clickhouse-server-project:8123
   ```
   Test Connection!
   
   -> Save

4. Check the project: on the left menu choose Home -> My Data -> clickhouse_server_project

5. Once the agents have finished, you can check the tables (and add descriptions if you'd like to): on the left menu choose Explore -> Databases -> clickhouse -> clickhouse_server_project (with "_3" at the end if you didn't need to create the connection) -> <table_name> (our tables are dim_date, dim_location, dim_organism, fact_observations)

6. Create tests: on the left menu choose Observability -> Data Quality -> Add a Test case

   We ran 3 test cases:
   1. Column based, in table dim_organism, on column organism_key, tested "values to be unique"
   2. Column based, in table fact_observation, on column location_key, tested "values to be not null"
   3. Column based, in table dim_date, on column season, tested "values to be in set", allowed values (winter, spring, summer, autumn)

7. Run the tests separately. 

   7.1. Make sure that you are on Observability -> Data Quality page 

   7.2. Then from the "Test Case Insights" section find column "Table" and click on one of the values (clickhouse_server_project.default.messud.<table_name>) on respective test case you want to test out. 

   7.3. Then click on the tab Pipelines

   7.4. You should see the row of the test case, where in column Actions click on the three dots and click Run.
   
   7.5 If the test was completed, you should see the count change on either success, failed, or warning square.
   
   PS! Sometimes the tests won't stop running. In that case try some of these: refresh the browser tab, run it again, and/or create a new test case.

## Visuals from OpenMetadata
### The tables and columns descriptions

![fact_observations](visuals/OMD_fact_observations_table.png)
![](visuals/OMD_fact_observations_columns.png)

![dim_organism](visuals/OMD_dim_organism_table.png)
![](visuals/OMD_dim_organism_columns1.png)
![](visuals/OMD_dim_organism_columns2.png)

![dim_date](visuals/OMD_dim_date_table.png)
![](visuals/OMD_dim_date_columns.png)

![dim_location](visuals/OMD_dim_location_table.png)
![](visuals/OMD_dim_location_columns.png)

### The results of the three test cases
![test_results](visuals/OMD_tests_results.png)


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
