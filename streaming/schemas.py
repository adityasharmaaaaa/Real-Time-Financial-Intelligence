from pyspark.sql.types import StructType, StructField, StringType, TimestampType

news_event_schema = StructType([
    StructField("id", StringType(), True),
    StructField("headline", StringType(), True),
    StructField("source", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])