from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Settings
from .dq import DataQualityError

PARTIAL_TABLES = ('fact_reviews_monthly', 'dim_product', 'dim_user_activity')
FINAL_TABLES = PARTIAL_TABLES + ('dim_date',)


def build_partials(clean: DataFrame) -> dict:
    """Aggregate one shard. Only sums and counts, so partials can be merged exactly."""
    base = clean.withColumn('review_month', F.trunc('review_date', 'month'))
    five = F.sum(F.when(F.col('score') == 5, 1).otherwise(0)).alias('five_star_count')
    one = F.sum(F.when(F.col('score') == 1, 1).otherwise(0)).alias('one_star_count')
    count = F.count('*').alias('review_count')
    rating = F.sum('score').alias('rating_sum')
    helpful = F.sum('helpful_num').alias('helpful_votes')
    votes = F.sum('helpful_den').alias('total_votes')

    fact = base.groupBy('product_id', 'review_month').agg(count, rating, helpful, votes, five, one)
    product = base.groupBy('product_id').agg(
        count, rating, helpful, votes, five, one,
        F.min('review_date').alias('first_review_date'),
        F.max('review_date').alias('last_review_date'),
    )
    user = (
        base.withColumn('user_key', F.sha2(F.col('user_id'), 256))
        .groupBy('user_key')
        .agg(count, rating, helpful, votes)
    )
    return {'fact_reviews_monthly': fact, 'dim_product': product, 'dim_user_activity': user}


def run_gold_partials(spark: SparkSession, settings: Settings, batch_id: str, shard: int, clean: DataFrame) -> dict:
    metrics = {}
    for name, df in build_partials(clean).items():
        out = settings.path('gold', '_partials', name, batch_id, f'shard={shard}')
        df.coalesce(1).write.mode('overwrite').parquet(str(out))
        metrics[f'{name}_rows'] = spark.read.parquet(str(out)).count()
    return metrics


def _read_partial(spark: SparkSession, settings: Settings, name: str, batch_id: str) -> DataFrame:
    return spark.read.parquet(str(settings.path('gold', '_partials', name, batch_id))).drop('shard')


def run_reduce(spark: SparkSession, settings: Settings, batch_id: str) -> dict:
    fact = _read_partial(spark, settings, 'fact_reviews_monthly', batch_id)
    product_p = _read_partial(spark, settings, 'dim_product', batch_id)
    user_p = _read_partial(spark, settings, 'dim_user_activity', batch_id)

    # Products never span shards, so shard results are already final for product tables.
    product = (
        product_p.withColumn('avg_rating', F.round(F.col('rating_sum') / F.col('review_count'), 3))
        .withColumn('five_star_share', F.round(F.col('five_star_count') / F.col('review_count'), 4))
        .withColumn('one_star_share', F.round(F.col('one_star_count') / F.col('review_count'), 4))
        .withColumn(
            'helpful_ratio',
            F.when(F.col('total_votes') > 0, F.round(F.col('helpful_votes') / F.col('total_votes'), 4)),
        )
    )
    # Users review across products, so the same user can appear in several shards: merge them.
    user = (
        user_p.groupBy('user_key')
        .agg(
            F.sum('review_count').alias('review_count'),
            F.sum('rating_sum').alias('rating_sum'),
            F.sum('helpful_votes').alias('helpful_votes'),
            F.sum('total_votes').alias('total_votes'),
        )
        .withColumn('avg_rating', F.round(F.col('rating_sum') / F.col('review_count'), 3))
    )

    bounds = fact.agg(F.min('review_month').alias('mn'), F.max('review_month').alias('mx')).first()
    if bounds['mn'] is None:
        raise DataQualityError('No data in gold partials')
    dim_date = (
        spark.range(1)
        .select(F.explode(F.sequence(F.lit(bounds['mn']), F.lit(bounds['mx']), F.expr('interval 1 month'))).alias('month_start'))
        .withColumn('year', F.year('month_start'))
        .withColumn('month', F.month('month_start'))
        .withColumn('quarter', F.quarter('month_start'))
        .withColumn('month_name', F.date_format('month_start', 'MMMM'))
        .withColumn('year_month', F.date_format('month_start', 'yyyy-MM'))
    )

    # Control totals: every clean silver row must be counted exactly once in every gold table.
    silver_rows = spark.read.parquet(str(settings.path('silver', 'reviews', batch_id))).count()
    totals = {
        'silver_rows': silver_rows,
        'fact_total': fact.agg(F.sum('review_count')).first()[0] or 0,
        'product_total': product.agg(F.sum('review_count')).first()[0] or 0,
        'user_total': user.agg(F.sum('review_count')).first()[0] or 0,
    }
    if len(set(totals.values())) != 1:
        raise DataQualityError(f'Control totals do not reconcile: {totals}')

    for name, df in {
        'fact_reviews_monthly': fact, 'dim_product': product,
        'dim_user_activity': user, 'dim_date': dim_date,
    }.items():
        df.coalesce(1).write.mode('overwrite').parquet(str(settings.path('gold', name)))
    return {**totals, 'products': product.count(), 'users': user.count()}