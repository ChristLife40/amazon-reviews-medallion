from pyspark.sql.types import StringType, StructField, StructType

RAW_COLUMNS = [
    'Id', 'ProductId', 'UserId', 'ProfileName', 'HelpfulnessNumerator',
    'HelpfulnessDenominator', 'Score', 'Time', 'Summary', 'Text',
]
RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in RAW_COLUMNS])