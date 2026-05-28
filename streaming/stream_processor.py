from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from schemas import news_event_schema
import os

# 1. Ensure checkpoint directories exist locally
os.makedirs("streaming/checkpoints/sources", exist_ok=True)
os.makedirs("streaming/checkpoints/windows", exist_ok=True)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("NewsStreamingProcessor") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# 2. Read from Kafka (Production config: latest)
raw_kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "news_events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON into structured DataFrame
parsed_df = raw_kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(F.from_json(F.col("value"), news_event_schema).alias("data")) \
    .select("data.*")

# 3. Aggressive State Management (Watermarks applied immediately)
watermarked_df = parsed_df.withWatermark("timestamp", "10 minutes")

# --- ANALYTICS ---

# 4. Events per source (Update Mode logic)
# Note: orderBy() is removed. Spark will just output changed rows,
# and the downstream sink (e.g., Redis/Postgres) will handle sorting.
events_per_source = watermarked_df.groupBy("source").count()

# 5. Sliding window counts
windowed_counts = watermarked_df \
    .groupBy(F.window("timestamp", "5 minutes", "1 minute")) \
    .count()

# --- OUTPUT SINKS ---

# Sink 1: Sources (using Update Mode & Checkpointing)
query_sources = events_per_source.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("checkpointLocation", "streaming/checkpoints/sources") \
    .start()

# Sink 2: Windows (using Update Mode & Checkpointing)
query_windows = windowed_counts.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("checkpointLocation", "streaming/checkpoints/windows") \
    .option("truncate", False) \
    .start()

spark.streams.awaitAnyTermination()