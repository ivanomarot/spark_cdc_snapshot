import defopt

from core.spark import build_spark_session
from core.cdc import CDCUpdatesProcessor
from core.sources import cdc


def main(*, input_table: str, output_table: str, raw_bucket: str, processing_date: str, partitioned: bool = False):
    CDCUpdatesProcessor(
        spark=build_spark_session(output_table), s3_bucket=raw_bucket, input_source=cdc[input_table],
        processing_date=processing_date, table=output_table).generate_snapshot(partitioned)


if __name__ == '__main__':
    defopt.run(main)
