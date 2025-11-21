from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
import pyarrow as pa
import os


# Load the catalog
# catalog = load_catalog(
#     name="rest",                # catalog name, arbitrary
#     type="rest",                # must match catalog type
#     uri="http://iceberg_rest:8181/",
#     warehouse="s3://messud-bucket/",
#     io_impl="org.apache.iceberg.aws.s3.S3FileIO",
#     s3={
#         "endpoint": "http://minio:9000",
#         "access-key-id": "minioadmin",
#         "secret-access-key": "minioadmin",
#         "path-style-access": True,
#     },
# )

# Load the catalog
catalog = load_catalog(name="rest")
# Define a schema with only one column
one_column_schema = Schema(
    NestedField(1, "sample_column", StringType(), required=False)
)

try:
    catalog.drop_table("messud_iceberg.aphis")
except NoSuchTableError:
    pass

# Create the table
table = catalog.create_table(
    identifier="messud_iceberg.aphis",
    schema=one_column_schema
)

data = pa.table({
    "sample_column": ["row1", "row2", "row3"]
})

table.append(data)
print("✅ Data appended successfully")
# Print where the table data will be stored
print(table.metadata.location)