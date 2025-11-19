SET allow_experimental_database_iceberg = 1;

DROP DATABASE IF EXISTS messud_iceberg;

-- 2. Create the database (backed by Iceberg REST catalog)
CREATE DATABASE messud_iceberg
ENGINE = DataLakeCatalog('http://iceberg_rest_project:8181/')
SETTINGS
    catalog_type = 'rest',
    warehouse = 'messud-bucket',
    storage_endpoint = 'http://minio_project:9000';




