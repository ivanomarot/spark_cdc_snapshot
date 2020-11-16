from datetime import datetime
from typing import List

import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException

from core.spark import load_dataframe, create_or_insert_table, get_table_partitions, catalog_table_exists
from core.utils import parse_date, run_multi_threaded_map, list_path_files


class CDCUpdatesProcessor:
    def __init__(
            self,
            spark: SparkSession = None,
            s3_bucket: str = None,
            input_source: dict = None,
            processing_date: str = None,
            table: str = None,
    ):
        self.spark = spark
        self.s3_bucket = s3_bucket
        self.input_source = input_source
        self.processing_date = processing_date
        self.table = table

    def generate_snapshot(self, partitioned: bool = False):
        snapshot = get_table_snapshot(
            spark=self.spark, s3_bucket=self.s3_bucket, table=self.table, source=self.input_source,
            processing_date=self.processing_date, date_partition=partitioned)
        self.write_output(snapshot, partitioned)

    def write_output(self, df: DataFrame, partitioned: bool = False):
        if partitioned:
            create_or_insert_table(
                spark=self.spark, df=df, table=self.table, s3_bucket=self.s3_bucket, needs_checkpoint=True,
                partition_keys=["dt"])
        else:
            create_or_insert_table(
                spark=self.spark, df=df, table=self.table, s3_bucket=self.s3_bucket, schema_overwrite=True,
                version=self.processing_date)


def get_table_snapshot(
        spark: SparkSession, s3_bucket: str, table: str, source: dict, processing_date: str,
        date_partition: bool = False) -> DataFrame:
    """
    :param spark: existing Spark session
    :param s3_bucket: s3 bucket name
    :param table: name of the table to be created in the catalog
    :param source: dictionary with cdc source settings
    :param processing_date: string with date to generate snapshot
    :param date_partition: specify if the output should be a date partition
    :returns DataFrame:
    """

    processing_date = parse_date(processing_date).date()
    if processing_date < source["cdc_start_date"]:
        raise ValueError("processing_date must be after the source cdc_start_date")

    last_date = None
    table_exists = catalog_table_exists(spark, table)
    if table_exists:
        if date_partition:
            last_date = parse_date(get_table_partitions(spark, table)[-1].split("=")[1]).date()
        else:
            last_date = parse_date(get_current_version(spark, table)).date()
        if processing_date < last_date:
            raise ValueError("processing_date must be after last_partition")

    # Define a new schema with DMS specific columns from the source schema
    updates_schema = T.StructType().add("Op", "string").add("cdc_timestamp", "string")
    for column in source["schema"]:
        updates_schema.add(column)

    # Clean the temporary updates directory
    spark.createDataFrame([], updates_schema) \
        .withColumn("file_number", F.lit(0)) \
        .withColumn("increasing_id", F.monotonically_increasing_id()) \
        .write.mode("overwrite").parquet(f"s3://{s3_bucket}/tmp/{table}_updates/")

    # Index update files into a temp folder to avoid loosing the
    # order to the records when Spark partitions the DataFrame
    def index_update_files(file_number: int, file_path: str, is_full_load: bool = False):
        df = spark.read.schema(updates_schema).parquet(file_path) \
            .withColumn("increasing_id", F.monotonically_increasing_id()) \
            .withColumn("file_number", F.lit(file_number))
        if is_full_load:
            df = df.withColumn("cdc_timestamp", F.lit(f"{str(processing_date)} 00:00:00.000000"))
        df.write.mode("append").parquet(f"s3://{s3_bucket}/tmp/{table}_updates/")

    # Index the initial full load files if the processing_date is equal to the CDC start date,
    # or the table does not exists.
    # Note: the cdc_timestamp is reset to avoid having CDC updates files with earlier timestamps
    if processing_date == source["cdc_start_date"] or not table_exists:
        full_load_files = get_cdc_files(processing_date, source["path"], full_load=True)
        run_multi_threaded_map(
            mapped_function=lambda args: index_update_files(*args),
            args_mapped_function=[(number, path, True) for number, path in enumerate(full_load_files)],
            thread_number=8)

    # Index the CDC update files between the last partition
    # created or the CDC start date and the processing_date.
    updates_files = get_cdc_files(processing_date, source["path"], last_date or source["cdc_start_date"])
    run_multi_threaded_map(
        mapped_function=lambda args: index_update_files(*args),
        args_mapped_function=[(number, path) for number, path in enumerate(updates_files)],
        thread_number=64)

    # Generate a new snapshot from the indexed files
    window = Window.partitionBy(*source["primary_keys"]) \
        .orderBy("cdc_timestamp", "file_number", "increasing_id")
    index_window = Window.partitionBy(*source["primary_keys"]) \
        .orderBy(F.col("ordered_index").desc())
    new_snapshot = spark.read.parquet(f"s3://{s3_bucket}/tmp/{table}_updates/") \
        .withColumn("ordered_index", F.row_number().over(window)) \
        .filter(F.to_date(F.col("cdc_timestamp")) <= str(processing_date)) \
        .filter(F.to_date(F.col("cdc_timestamp")) >= str(last_date or source["cdc_start_date"])) \
        .withColumn("row_number", F.row_number().over(index_window)) \
        .filter(F.col("row_number") == 1)
    if date_partition:
        new_snapshot = new_snapshot.withColumn("dt", F.to_date(F.col(source['partition_date']))) \
            .filter(F.col("dt") == str(processing_date))

    # Generate an old_snapshot from the partitions that have to be updated
    # or get an empty DataFrame if the table does not exist
    final_snapshot_schema = T.StructType()
    for column in source["schema"]:
        final_snapshot_schema.add(column)
    if date_partition:
        final_snapshot_schema.add("dt", "date")
    old_snapshot = spark.createDataFrame([], final_snapshot_schema)
    if table_exists:
        if date_partition:
            old_snapshot = spark.read.table(table).filter(F.col("dt") == str(processing_date))
        else:
            last_table_location = f"s3://{s3_bucket}/{table}/version={str(last_date)}/"
            old_snapshot = load_dataframe(spark, last_table_location, source["schema"])

    # Merge both old and new snapshots to create a new snapshot
    # of the partition to be overwritten into the final table
    conditions = [old_snapshot[name] == new_snapshot[name] for name in source["primary_keys"]]
    fields = map(lambda field: F.coalesce(new_snapshot[field.name], old_snapshot[field.name]).alias(field.name),
                 final_snapshot_schema.fields)
    final_snapshot = old_snapshot.join(new_snapshot, conditions, how="outer") \
        .filter(new_snapshot.Op.isNull() | (new_snapshot.Op != 'D')) \
        .select(*fields)
    if date_partition:
        final_snapshot = final_snapshot.repartition("dt")
    return final_snapshot


def get_cdc_files(
        processing_date: datetime.date, cdc_updates_path: str, start_date: datetime.date = None,
        full_load: bool = False) -> List[str]:
    """
    :param processing_date: date to generate snapshot
    :param cdc_updates_path: S3 prefix containing the CDC files exported by DMS
    :param start_date: date when CDC files started to get exported
    :param full_load: flag to get the list of files from the initial full load
    :returns list: strings with full s3 prefixes of the files
    """
    all_cdc_files = list_path_files(cdc_updates_path)
    if full_load:
        return sorted([path for path in all_cdc_files if "LOAD" in path])
    else:
        if not start_date:
            raise ValueError("start_date is required to get updates files")
        return sorted(
            [path for path in all_cdc_files
             if ("LOAD" not in path) and
             (datetime.strptime(path.split('/')[-1][0:8], "%Y%m%d").date() >= start_date) and
             (datetime.strptime(path.split('/')[-1][0:8], "%Y%m%d").date() <= processing_date)]
        )


def get_current_version(spark: SparkSession, table: str):
    """
    :param spark: existing Spark session
    :param table: name of the table stored in the catalog
    :returns string: current version identifier of the table or None if table does not exists
    """
    query = f"DESCRIBE EXTENDED {table}"
    try:
        location = spark.sql(query) \
            .filter(F.col('col_name') == 'Location') \
            .collect()[0].data_type
        return location.split('version=')[1][0:10]
    except AnalysisException as e:
        if "Table or view not found" in str(e):
            return None
        else:
            raise e
