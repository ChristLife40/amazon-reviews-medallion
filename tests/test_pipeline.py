from reviews_pipeline import jobs
from reviews_pipeline.config import Settings


def test_sharded_gold_equals_unsharded_gold(spark, bronze_df, tmp_path):
    results = {}
    for n in (1, 3):
        settings = Settings(data_root=tmp_path / f'run{n}')
        bronze_df.write.parquet(str(settings.path('bronze', 'reviews', 'b1')))
        for k in range(n):
            jobs.job_shard(settings, 'b1', k, n, spark=spark)
        jobs.job_reduce(settings, 'b1', spark=spark)
        fact = spark.read.parquet(str(settings.path('gold', 'fact_reviews_monthly')))
        results[n] = sorted(tuple(r) for r in fact.collect())
    assert results[1] == results[3]
    assert sum(r[2] for r in results[1]) == 3  # review_count column