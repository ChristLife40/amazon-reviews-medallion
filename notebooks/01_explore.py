from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from reviews_pipeline.schemas import RAW_SCHEMA

s = (SparkSession.builder.master('local[*]').config('spark.ui.enabled', 'false')
     .config('spark.driver.memory', '2g').getOrCreate())
df = (s.read.option('header', True).option('multiLine', True).option('quote', '"')
      .option('escape', '"').schema(RAW_SCHEMA).csv('data/landing/amazon-fine-food/Reviews.csv'))
print('rows:', df.count())
df.select([F.sum(F.col(c).isNull().cast('int')).alias(c) for c in df.columns]).show()
df.groupBy('Score').count().orderBy('Score').show()
print('duplicate (user, product, time) keys:',
      df.groupBy('UserId', 'ProductId', 'Time').count().filter('count > 1').count())
print('helpfulness numerator > denominator:',
      df.filter(F.col('HelpfulnessNumerator').cast('int') > F.col('HelpfulnessDenominator').cast('int')).count())
print('time range (epoch):', df.select(F.min(F.col('Time').cast('long')), F.max(F.col('Time').cast('long'))).first())
s.stop()
