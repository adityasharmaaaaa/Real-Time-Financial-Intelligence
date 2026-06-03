import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from schemas import news_event_schema

# 1. Ensure checkpoint and lakehouse directories exist
os.makedirs("lakehouse/checkpoints/bronze", exist_ok=True)
os.makedirs("lakehouse/bronze/news", exist_ok=True)

# 2. Initialize Spark Session with Delta extensions
spark = SparkSession.builder \
    .appName("BronzeIngestionPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 3. Read raw stream from Kafka
raw_kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "news_events") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse JSON
parsed_df = raw_kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(F.from_json(F.col("value"), news_event_schema).alias("data")) \
    .select("data.*")

# 5. Write Stream to Bronze Delta Table
bronze_query = parsed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "lakehouse/checkpoints/bronze") \
    .start("lakehouse/bronze/news")

print("Streaming Kafka events to Bronze Delta Table...")
bronze_query.awaitTermination()