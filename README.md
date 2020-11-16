# Spark CDC Snapshot Generator

### Description

This project includes production grade code for merging CDC files from DMS into a table in S3 backed by Glue 
using pure [Spark](https://spark.apache.org/) code.

In order to use this project you need to create in advance:

- RDS Database with binary logging enabled.
- DMS migration task ingesting CDC files from RDS to S3 in Parquet format without date partitioning.
- EMR cluster with version 6.1, EMRFS consistent view and the Python requirements installed.
- S3 bucket

### How it works?

1. Creates a list of the files exported by DMS from the date where the task started until the provided run date.
2. Index the files and store them in a temp S3 directory (this is done to preserve the order of the updates).
3. Read the last version of the snapshot if exists as a DataFrame.
5. Use the primary keys and cdc_timestamps to generate a snapshot from the indexed files and the previous snapshot
(if exists).
6. Store the final snapshot in S3 as a Glue catalog table or inserts the last day partition in an existing Glue 
catalog table.

**Features**:

- The solution enforces a schema strictly to keep consistency of the snapshot datasets.
- It creates a Glue catalog table in two ways:
    - A flat table with with a versioned s3 prefix.
    - A date partitioned table with s3 partitioned with format `<date_column_name>=YYYY-mm-dd`
- Written in pure Pyspark API
- Ensures order of transactions as originally exported by DMS

*Note 1*: `bootstrap.sh` can be used to install the Python modules with an EMR boostrap action.

*Note 2*: For the DMS task, the following S3 target endpoint settings can be used:

```
dataFormat=parquet;datePartitionEnabled=false;ParquetTimestampInMillisecond=true;ParquetVersion=parquet_2_0;timestampColumnName=cdc_timestamp;
```
*Note 3*: The generator accounts for the following edge cases seen in production:

**Problem**: *When the source database has a high rate of changes, the CDC files exported by DMS may contain updates from
the same key with equal cdc_timestamp. This is a problem in Spark because the partitioner does not keep the order of
the records inside of the parquet files when they are read them from a parent path in S3. The records are distributed
by the Spark executor into multiple partitions in the cluster.*

**Solution**: The generator class includes a pre-processing task where it creates a list of files to be processed and 
relies on Spark thread safe capabilities to read each file in the list with a single core using a multi-threaded map. 
The files are indexed and sent to a temporary destination path where they can be processed safely using the Spark API.

**Problem**: *When DMS exports a full load followed by CDC replication, there can be cases when updates may have a
cdc_timestamp value older than the original key exported during the full load.*

**Solution**: If there is no pre-existing snapshot or the processing date is equal to the CDC start date, the
generator will identify the files containing the original keys from the full load and reset their timestamp so that
they are always the first version of every key exported.

*Note 4*: For the EMR cluster in version 6.1, the configurations below were tested to work well for this use case.

```
[{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}},{"Classification":"yarn-site","Properties":{"yarn.nodemanager.pmem-check-enabled":"false","yarn.nodemanager.vmem-check-enabled":"false"}},{"Classification":"spark-defaults","Properties":{"spark.sql.sources.partitionOverwriteMode":"dynamic","spark.driver.memory":"4743M","spark.sql.legacy.parquet.datetimeRebaseModeInWrite":"CORRECTED","spark.sql.legacy.parquet.datetimeRebaseModeInRead":"LEGACY","spark.driver.cores":"4","spark.default.parallelism":"600","spark.sql.adaptive.enabled":"true","spark.sql.adaptive.coalescePartitions.initialPartitionNum":"1200","spark.task.maxFailures":"10"}},{"Classification":"emrfs-site","Properties":{"fs.s3.consistent.metadata.etag.verification.enabled":"false","fs.s3.consistent":"true","fs.s3.consistent.metadata.tableName":"EmrFSMetadata"}}]
```

### Run

Clone this repository to your local Python IDE.

Modify the file `core/sources.py` and include the details of the tables being imported by DMS in the `cdc` dictionary.

Example:

```
cdc = {
    "employees": {
        "path": "s3://sprkcdc-bucket/dms/employees/employees/",
        "primary_keys": ["emp_no"],
        "partition_date": "hire_date",
        "cdc_start_date": date(2020, 11, 13),
        "schema": T.StructType()
        .add("emp_no", "integer")
        .add("birth_date", "date")
        .add("first_name", "string")
        .add("last_name", "string")
        .add("gender", "string")
        .add("hire_date", "date")
    }
}
```

Options:

```
:path string: Path where DMS is writting the files of the table.
:primary_keys list[string]: List of string with the column names of the primary keys of the table.
:partition_date string: Optional column name to be used to partition the output of the snapshot. It is only used
when the job is executed with the partitioned flag
:cdc_start_date date: Date when the DMS task was started
:schema StructType: schema of the table to be enforced by Spark
```

Run the job from your IDE console.

Example:

```
S3_BUCKET="sprkcdc-bucket" EMR_CLUSTER="j-24QXUTH22LQFZ" TABLE="employees" RUN_DATE="2020-11-13" make run
```

Options:

```
EMR_CLUSTER: EMR cluster ID
S3_BUCKET: S3 bucket name
TABLE: Table name
RUN_DATE: Date to generate the snapshot
DATABSE (Optional): Glue database, if not provided will use 'default'
```

When the job finishes you will see a Glue catalog table being written as `<database>.<table>_snapshot`. You can 
query this table in Athena, Spark of Redshift Spectrum and see the contents of the table.