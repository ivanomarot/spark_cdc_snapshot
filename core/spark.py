from typing import List, Optional

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType


def build_spark_session(
        application_name: str, master: str = 'yarn', job_parallelism: Optional[int] = None,
        job_shuffle_partitions: Optional[int] = None) -> SparkSession:
    """
    :param application_name: name of the spark application
    :param master: URI of the master
    :param job_parallelism: default number of partitions in RDDs returned by transformations like join
    :param job_shuffle_partitions: umber of partitions that are used when shuffling data for joins or aggregations
    :returns SparkSession:
    """
    conf: SparkConf = SparkConf() \
        .setAppName(application_name) \
        .setMaster(master)

    if job_parallelism:
        conf.set('spark.default.parallelism', job_parallelism)

    if job_shuffle_partitions:
        conf.set('spark.sql.shuffle.partitions', job_shuffle_partitions)

    spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

    return spark


def get_table_partitions(spark: SparkSession, table: str) -> List[str]:
    """
    :param spark: existing Spark session
    :param table: name of the table stored in the catalog
    :returns list: table partitions as strings
    """
    sql_query = f'SHOW PARTITIONS {table}'
    partition_list = spark.sql(sql_query)
    return [row.asDict()['partition'] for row in partition_list.collect()]


def catalog_table_exists(spark: SparkSession, table: str):
    """
    :param spark: existing Spark session
    :param table: name of the table stored in the catalog
    :returns list: table partitions as strings
    """
    database_name = None
    if "." in table:
        database_name, table = table.split('.', 1)
    table_list = spark.catalog.listTables(database_name)
    return len(list(filter(lambda t: t.name == table, table_list))) > 0


def overwrite_full_table(
        df: DataFrame, table: str, s3_bucket: str, output_format: str = 'parquet',
        max_records_per_file: int = None, version: Optional[str] = None,
        partition_keys: Optional[List[str]] = None):
    """
    :param df: DataFrame to be saved
    :param table: name of the table to be created in the catalog
    :param s3_bucket: s3 bucket name
    :param output_format: output file format
    :param max_records_per_file: maximum number of records per output file
    :param version: version identifier for versioned datasets
    :param partition_keys: list of partition columns for the table
    """
    path = f's3://{s3_bucket}/{table}/version={version}/' if version else f's3://{s3_bucket}/{table}/'
    df_writer = df.write \
        .format(output_format) \
        .mode('overwrite') \
        .option('path', path)
    if max_records_per_file:
        df_writer = df_writer.option('maxRecordsPerFile', max_records_per_file)
    df_writer.saveAsTable(table, partitionBy=partition_keys)


def create_or_insert_table(
        spark: SparkSession, df: DataFrame, table: str, s3_bucket: str, needs_checkpoint: bool = False,
        partition_keys: Optional[List[str]] = None, output_format: str = 'parquet', schema_overwrite: bool = False,
        version: Optional[str] = None):
    """
    :param spark: existing Spark session
    :param df: DataFrame to be saved
    :param table: name of the table to be created in the catalog
    :param s3_bucket: s3 bucket name
    :param needs_checkpoint: use an intermediate temporary location before saving the table
    :param partition_keys: list of partition columns for the table
    :param output_format: output file format
    :param schema_overwrite: use the saveAsTable api to overwrite the schema of the table
    :param version: version identifier for versioned datasets
    """
    if partition_keys and version and not schema_overwrite:
        raise ValueError("schema_overwrite must be set if table is both versioned and partitioned")
    if catalog_table_exists(spark, table):
        if needs_checkpoint:
            df.write.mode("overwrite") \
                .format(output_format) \
                .save(f"s3://{s3_bucket}/checkpoints/{table}/")
            df = spark.read \
                .format(output_format) \
                .load(f"s3://{s3_bucket}/checkpoints/{table}/")
        if schema_overwrite:
            overwrite_full_table(df, table, s3_bucket, output_format=output_format, version=version,
                                 partition_keys=partition_keys)
        else:
            df.write.format(output_format) \
                .mode("overwrite") \
                .insertInto(table, overwrite=True)
    else:
        overwrite_full_table(df, table, s3_bucket, output_format=output_format, version=version,
                             partition_keys=partition_keys)


def load_dataframe(spark: SparkSession, path: str, schema: StructType, data_format: str = 'parquet') -> DataFrame:
    """
    :param spark: existing Spark session
    :param path: S3 prefix to be loaded
    :param schema: schema used to parse the S3 files
    :param data_format: expected file format
    :returns DataFrame:
    """
    try:
        return spark.read.load(
            path,
            format=data_format,
            schema=schema)
    except AnalysisException as e:
        if "Path does not exist" in str(e):
            # No data to load, create empty data frame
            return spark.createDataFrame([], schema)
        raise e
