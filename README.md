# The repository for the data engineering project for group 7 on the topic of the health of bee populations in US counties.


<!-- The report for project 3 is in [P3_report.pdf](P3_report.pdf).) -->

## Team members
* Remi Raugme
* Annaliisa Vask
* Selene Margaret Pruuden
* Victoria Prins

# Introduction

Bees are important pollinators and thus are an integral part of terrestrial ecosystems. Due to anthropogenic factors, such as climate change, pollution and habitat destruction, bee populations are in a decline globally. This issue is made worse due to the spread of pests and viruses which wipe out bee colonies. These pests can be Varroa mites and Nosema fungi which often co-occur and can cause devastating losses among bee colonies. In order to better protect the functioning of our ecosystems, it is of high importance to establish methods that can help predict which environments are most suitable for bees. Additionally, these pests and viruses have an effect on the food industry, as their infections can cause financial loss for beekeepers. With our project we are combining two datasets to describe patterns behind successful beekeeping.

The previous reports can be found in Reports folder [P1_Group_07.pdf](Reports/P1_Group_07.pdf) for project 1 and [P2_Group_07.pdf](Reports/P2_Group_07.pdf) for project 2.

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
docker exec -it clickhouse-server-project clickhouse-client --multiquery --queries-file=/sql/create_db_and_tables.sql
```

4. Enter the Airflow scheduler container and install dbt dependencies
```bash
docker exec -it airflow-scheduler-project bash
# then, inside the container:
cd /dbt
dbt deps
```
5. Go to http://localhost:8080 log in. The user is `airflow` and password is `airflow`. Enable all the DAGs.

6. Inserting data using airflow
   * Manually create 2 new folders with the names of `aphis` and `gbif` into the following path: `docker_base/sample_data/`. 
   * Then move the file named `APHIS...short.csv` into `aphis` folder and `GBIF...short.txt` into `gbif` folder.

7. The password for clickhouse webUI is `12345678` and user is default.

### 8 Running Iceberg (it is important to run all the 1.-7. steps beforehand)
   * Got to http://localhost:9101 log in to MinIO. The user is `minioadmin` and password is `minioadmin`.
   * Create a bucket called `messud-bucket`. 
   * Run the following command to create iceberg database and connection
   ```bash
   docker exec -it clickhouse-server-project clickhouse-client --multiquery --queries-file=/sql/iceberg_create_db_and_tables.sql
   ```

### 9 Running Clickhouse roles and views (it is important to run all the 1.-7. steps beforehand)
   * Run this command for roles
   ```bash
   docker exec -it clickhouse-server-project clickhouse-client --multiquery --queries-file=/sql/clickhouse_roles.sql
   ```
   * Run this command for views
   ```bash
   docker exec -it clickhouse-server-project clickhouse-client --multiquery --queries-file=/sql/clickhouse_views.sql
   ```

### 10 Running OpenMetadata (it is important to run all the 1.-7. steps beforehand)
PS! There might rise an issue that elastic search container won't stay running. If this happens then go in Docker UI Settings -> Resources and increase the Memory Limit value.

1. Create a role that can access database on OpenMetadata
```bash
   docker exec -it clickhouse-server-project clickhouse-client --multiquery --queries-file=/sql/omd_role.sql
   ```

2. Go to http://localhost:8585 and log in. The username is admin@open-metadata.org and password is admin.

3. In the OpenMetadata UI go to Settings -> Services -> Databases and open clickhouse_server_project. 

   If you can't see the aforementioned service name then create the connection yourself following the next step on 3.1!

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

5. Once the agents have finished, you can check the tables (and add descriptions if you'd like to): on the left menu choose Explore -> Databases -> clickhouse -> clickhouse_server_project -> <table_name> (our gold layer tables are dim_date, dim_location, dim_organism, fact_observations)

6. Create tests: on the left menu choose Observability -> Data Quality -> Add a Test case

   We ran 3 test cases:
   1. Column based, in table dim_organism, on column organism_key, tested "values to be unique"
   2. Column based, in table fact_observation, on column location_key, tested "values to be not null"
   3. Column based, in table dim_date, on column season, tested "values to be in set", allowed values (winter, spring, summer, autumn)

7. Run the tests separately. 

   7.1. Make sure that you are on Observability -> Data Quality page 

   7.2. Then from the "Test Case Insights" section find column "Table" and click on one of the values (clickhouse_server_project.default.messud.<table_name>) on the respective test case you want to test out. 

   7.3. Then click on the tab Pipelines

   7.4. You should see the row of the test case, where in column Actions click on the three dots and then click Run.
   
   7.5 If the test was completed, you should see the count change on either success, failed, or warning square.
   
   PS! Sometimes the tests won't stop running. In that case try some of these: refresh the browser tab, rerun it, and/or create a new test case.

Scroll down to the "Visuals from OpenMetadata" section to see screenshots of the setup.

### 11 Running Apache Superset (it is important to run all the 1.-7. and 10. steps beforehand)
#### 11.1 Connecting Superset with ClickHouse

1. Create a role that will be used to access Apache superset

```bash
   docker exec -it clickhouse-server-project clickhouse-client --multiquery --queries-file=/sql/superset_role.sql
   ```
2. Go to http://localhost:8088. Log in with the default credentials (username: admin; password: admin)

3. Once you have accessed the superset UI it is time to connect it to the database:
   
    3.1 Go to datasets and click on "+" then on "Data" and finally on "Connect Database"
   
    3.2 In the "Connect a database" window search for "ClickHouse Connect (Superset) in the supported databases list
   
    3.3 Next, connect as follows:
   
    ```bash
    Host: clickhouse-server-project
    Port: 8123
    Database name: messud
    Username: user_superset
    Password: ss_very_secret_password
    ```
You now have successfully connected ClickHouse and Superset. 

The dashboard included a filter for the average pest score value, which enables users to select ranges for the pest scores.

Scroll down for the visuals of Superset.

##### Results interpretation
1. Business question 1: From the three counties with the highest average virus prevalence - how many bees were detected during the year 2024?

According to the table BQ1_table, the three counties that had the highest average virus prevalence were Jefferson (60% of bees infected), Washington (59%) and Prince George (58%). From these counties, 63, 53 and 1596 bees were detected respectively.  The differences between bee occurrences are quite large, indicating that there may be other variables that affect the number of bees.

2. Business question 2:How many bee occurrences are there in the five counties with the fewest Varroa mites?

The graph BQ2_bubblecharts depicts 10 counties with the fewest Varroa mites. The five counties with the fewest Varroa mites were: Swift, Madera, Carver, Jones and Maui. From these five, Maui had both the highest number of bee occurrences (~400) and the highest average number of Varroa occurrences (2.25).

3. Business question 6: Which county is most popular for beekeeping and which is most safe from pests?

In the table BQ6_chart it can be seen that Prince George's is the most popular county for beekeeping, as 57,000 bee occurrences were detected. However, it can be seen from the chart that it also has one of the highest overall pest scores of the counties with highest bee occurrences. This could indicate that pests are easily spread in areas with large numbers of bee colonies. In the BQ6_lineechart, it can be seen that slight rises in pest scores across counties can cause dips in the number of bees detected, and vice-versa.

#### 11.2 Connecting Superset with OpenMetadata
Unfortunately, the latest version of Superset does not support stable connection to OpenMetadata, which is why the pipeline uses an older version. However, despite testing with various different versions of both Superset and OpenMetadata, the Superset dashboards did not appear in OpenMetadata. There were no connection issues nor any errors logs. 

These were the steps used to connect Superset with OpenMetadata:

1. Go to http://localhost:8585. The username is admin@open-metadata.org and password is admin.
    1.1 From the left side menu, open "Settings". Then proceed to Services -> Dashboards.

    1.2 Click "Add New Service"
   
    1.3 From the Dashboard services, select Superset.
   
    1.4 Next, connect with the service as follows:
   
    ```bash
    Host And Port: http://superset_app:8088
    Superset Connection: SupersetApiConnection
    Provider: db
    Username: admin
    Password: admin
    ```
    1.4.1 Alternatively, you can create another user. However, if you intend to go this route, you must either create the graphs and dashboards with the said user OR give the said user ownership of the dashboard.
    ```bash'
    # entering the superset container
    
    docker exec -it superset_app bash
    
    # create the custom user
    superset fab create-user \
    --username om_user \
    --firstname Open \
    --lastname Metadata \
    --email om_user@example.com \
    --password "metadata_very_secret_password" \
    --role Admin
    ``
    Log in with these credentials instead.
    1.5 Test the connection.
    1.6 Connnect

### View of Superset in OpenMetadata
![Superset OMD](visuals/OMD_superset.png)

## Visuals from OpenMetadata

<details>
<summary> Click Me to see content </summary>

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

</details>

## Visuals from Superset

<details>
<summary> Click Me to see content </summary>

### Superset Dashboard 
![Superset dashboard](visuals/Messud_dashboard.png)

### Business question 2: How many bee occurrences are there in the 10 counties with the fewest Varroa mites? 
![Business q2](visuals/BQ2_bubble_chart.png)
### Business question 6: Which county is most popular for beekeeping and which is most safe from pests? 
![Business q3](visuals/BQ6_linechart.png)

</details>

## Visuals from Airflow (project 2)
<details>
<summary> Click Me to see content </summary>

### The DAGs used in the project
![Airflow DAGs](visuals/airflow_dags.png)
### Aphis DAG
![Aphis DAG](visuals/airflow_aphis_dag.png)
### GBIF DAG
![GBIF DAG](visuals/airflow_gbif_dag.png)
### dbt DAG
![dbt DAG](visuals/airflow_dbt_dag.png)

</details>

## Results for analytical queries (project 2)

<details>
<summary> Click Me to see content </summary>

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

</details>

## Known issues (project 2)

### Duplicate ingestion
Sadly we couldn't get non duplicate ingestion working properly. Found a [webpage](https://cc.davelozinski.com/sql/fastest-way-to-insert-new-records-where-one-doesnt-already-exist)
that outlined 4 methods to achieve it and tried all of them except MERGE. They either didn't work on the first ingestion or didn't work after the first ingestion — either still adding everything or adding nothing.
