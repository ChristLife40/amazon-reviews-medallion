from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .schemas import RAW_SCHEMA


def build_bronze(spark: SparkSession, csv_path, batch_id: str) -> DataFrame:
    raw = (
        spark.read.option('header', True)
        .option('multiLine', True)
        .option('quote', '"')
        .option('escape', '"')
        .schema(RAW_SCHEMA)
        .csv(str(csv_path))
    )
    return (
        raw.withColumn('source_file', F.input_file_name())
        .withColumn('batch_id', F.lit(batch_id))
        .withColumn('ingest_ts', F.date_format(F.current_timestamp(), 'yyyy-MM-dd HH:mm:ss'))
    )


def write_bronze(df: DataFrame, out_dir) -> None:
    df.repartition(2).write.mode('overwrite').parquet(str(out_dir))