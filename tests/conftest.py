
import pytest
from pyspark.sql import SparkSession

from reviews_pipeline.schemas import RAW_COLUMNS

COLUMNS = RAW_COLUMNS + ['source_file', 'batch_id', 'ingest_ts']
TS = '2024-01-01 00:00:00'
ROWS = [
    ('1', 'P1', 'U1', 'amy', '1', '2', '5', '1303862400', 'Great', 'Nice<br />taste', 'f.csv', 'b1', TS),
    ('2', 'P1', 'U2', 'bob', '0', '0', '1', '1303862500', 'Bad', 'Awful', 'f.csv', 'b1', TS),
    ('3', 'P1', 'U1', 'amy', '1', '2', '5', '1303862400', 'Great', 'Nice <br />taste', 'f.csv', 'b1', TS),
    ('4', 'P2', 'U3', 'cy', '0', '0', '7', '1303862600', 'Odd', 'Score out of range', 'f.csv', 'b1', TS),
    ('5', 'P2', 'U4', 'di', '3', '2', '4', '1303862700', 'Odd', 'Helpful above total', 'f.csv', 'b1', TS),
    ('6', 'P3', '', 'ed', '0', '0', '3', '1303862800', 'Odd', 'Missing user', 'f.csv', 'b1', TS),
    ('7', 'P2', 'U1', 'amy', '0', '1', '4', '1303862900', 'Fine', 'Good', 'f.csv', 'b1', TS),
]


@pytest.fixture(scope='session')
def spark():
    session = (
        SparkSession.builder.master('local[2]').appName('tests')
        .config('spark.sql.shuffle.partitions', '2')
        .config('spark.sql.session.timeZone', 'UTC')
        .config('spark.ui.enabled', 'false')
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture()
def bronze_df(spark):
    return spark.createDataFrame(ROWS, schema=COLUMNS)