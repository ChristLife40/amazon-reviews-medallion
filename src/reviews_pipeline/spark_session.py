from pyspark.sql import SparkSession

from .config import Settings


def get_spark(settings: Settings, app_name: str = 'reviews-pipeline') -> SparkSession:
    return (
        SparkSession.builder.master('local[*]')
        .appName(app_name)
        .config('spark.sql.shuffle.partitions', str(settings.shuffle_partitions))
        .config('spark.driver.memory', settings.driver_memory)
        .config('spark.sql.session.timeZone', 'UTC')
        .config('spark.ui.enabled', 'false')
        .getOrCreate()
    )
    