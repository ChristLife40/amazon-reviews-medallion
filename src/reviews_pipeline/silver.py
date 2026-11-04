from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .config import Settings


def _clean_text(col: str):
    no_tags = F.regexp_replace(F.col(col), '<[^>]+>', ' ')
    return F.trim(F.regexp_replace(no_tags, r'\\s+', ' '))


def transform_silver(bronze: DataFrame, shard: int, shard_count: int):
    """Return (clean, rejects) for one shard of the bronze data."""
    typed = bronze.select(
        F.col('Id').cast('long').alias('review_id'),
        F.trim(F.col('ProductId')).alias('product_id'),
        F.trim(F.col('UserId')).alias('user_id'),
        F.col('ProfileName').alias('profile_name'),
        F.col('HelpfulnessNumerator').cast('int').alias('helpful_num'),
        F.col('HelpfulnessDenominator').cast('int').alias('helpful_den'),
        F.col('Score').cast('int').alias('score'),
        F.col('Time').cast('long').cast('timestamp').alias('review_ts'),
        _clean_text('Summary').alias('summary'),
        _clean_text('Text').alias('text'),
        F.col('source_file'),
        F.col('batch_id'),
        F.to_timestamp(
            F.regexp_replace(F.substring(F.col('ingest_ts'), 1, 19), 'T', ' ')
        ).alias('ingest_ts'),
    ).withColumn('review_date', F.to_date('review_ts'))

    # Sharding: every review of a product goes to exactly one shard.
    typed = typed.filter(F.pmod(F.hash('product_id'), F.lit(shard_count)) == F.lit(shard))

    missing = (
        F.col('product_id').isNull() | (F.col('product_id') == '')
        | F.col('user_id').isNull() | (F.col('user_id') == '')
        | F.col('review_ts').isNull()
    )
    bad_score = F.col('score').isNull() | ~F.col('score').between(1, 5)
    bad_help = (
        F.col('helpful_num').isNull() | F.col('helpful_den').isNull()
        | (F.col('helpful_num') < 0) | (F.col('helpful_num') > F.col('helpful_den'))
    )
    flagged = typed.withColumn(
        'reject_reason',
        F.when(missing, 'missing_key_or_time')
        .when(bad_score, 'invalid_score')
        .when(bad_help, 'invalid_helpfulness'),
    )
    rejected = flagged.filter(F.col('reject_reason').isNotNull())
    valid = flagged.filter(F.col('reject_reason').isNull()).drop('reject_reason')

    window = Window.partitionBy('user_id', 'product_id', 'review_ts').orderBy(
        F.col('ingest_ts').desc(), F.col('review_id').asc()
    )
    ranked = valid.withColumn('rn', F.row_number().over(window))
    clean = ranked.filter(F.col('rn') == 1).drop('rn')
    dups = ranked.filter(F.col('rn') > 1).drop('rn').withColumn('reject_reason', F.lit('duplicate'))
    return clean, rejected.unionByName(dups)


def run_silver_shard(spark: SparkSession, settings: Settings, batch_id: str, shard: int, shard_count: int):
    """Write silver + rejects for one shard. Returns (metrics, cached clean DataFrame)."""
    bronze = spark.read.parquet(str(settings.path('bronze', 'reviews', batch_id)))
    clean, rejects = transform_silver(bronze, shard, shard_count)
    clean, rejects = clean.cache(), rejects.cache()

    out_clean = settings.path('silver', 'reviews', batch_id, f'shard={shard}')
    out_rej = settings.path('silver', 'rejects', batch_id, f'shard={shard}')
    clean.coalesce(1).write.mode('overwrite').parquet(str(out_clean))
    rejects.coalesce(1).write.mode('overwrite').parquet(str(out_rej))

    reasons = {
        r['reject_reason']: r['n']
        for r in rejects.groupBy('reject_reason').agg(F.count('*').alias('n')).collect()
    }
    rows_clean = clean.count()
    metrics = {
        'rows_in': rows_clean + sum(reasons.values()),
        'rows_clean': rows_clean,
        'rows_rejected': sum(reasons.values()),
        'reject_reasons': reasons,
    }
    rejects.unpersist()
    return metrics, clean