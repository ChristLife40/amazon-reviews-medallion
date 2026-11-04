from reviews_pipeline.silver import transform_silver


def test_clean_rows_and_reject_reasons(bronze_df):
    clean, rejects = transform_silver(bronze_df, shard=0, shard_count=1)
    counts = rejects.groupBy('reject_reason').count().collect()
    reasons = {r['reject_reason']: r['count'] for r in counts}
    assert clean.count() == 3
    assert reasons == {
        'invalid_score': 1, 'invalid_helpfulness': 1, 'missing_key_or_time': 1, 'duplicate': 1,
    }


def test_html_is_removed_and_duplicate_keeps_lowest_id(bronze_df):
    clean, _ = transform_silver(bronze_df, shard=0, shard_count=1)
    row = clean.filter('review_id = 1').first()
    assert row['text'] == 'Nice taste'
    assert clean.filter('review_id = 3').count() == 0


def test_shards_partition_the_data(bronze_df):
    full, _ = transform_silver(bronze_df, 0, 1)
    expected = {r['review_id'] for r in full.collect()}
    seen = set()
    for k in range(3):
        clean, _ = transform_silver(bronze_df, k, 3)
        ids = {r['review_id'] for r in clean.collect()}
        assert seen.isdisjoint(ids)
        seen |= ids
    assert seen == expected