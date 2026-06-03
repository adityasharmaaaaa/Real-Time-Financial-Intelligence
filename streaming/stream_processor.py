from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from schemas import news_event_schema
import os
import logging

# --- 1. Load the AI Model ---
print("Loading FinBERT Model... (This may take a minute on boot)")
from transformers import pipeline
# We use a specific financial sentiment model
sentiment_pipeline = pipeline("sentiment-analysis", model="ProsusAI/finbert")

# Define the UDF to apply the model to our text
@F.udf(StringType())
def get_sentiment(headline):
    try:
        # FinBERT returns a list of dicts, e.g., [{'label': 'positive', 'score': 0.89}]
        result = sentiment_pipeline(headline)[0]
        return result['label']
    except Exception as e:
        return "neutral"

# --- 2. Standard Setup ---
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.makedirs("streaming/checkpoints/sources", exist_ok=True)
os.makedirs("streaming/checkpoints/sentiment", exist_ok=True)

spark = SparkSession.builder \
    .appName("NewsStreamingProcessor") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Reduce Spark's messy log output so we can see our tables easily
spark.sparkContext.setLogLevel("WARN")

# --- 3. Read & Parse ---
raw_kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "news_events") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = raw_kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(F.from_json(F.col("value"), news_event_schema).alias("data")) \
    .select("data.*")

# --- 4. Apply AI Transformation ---
# This creates a new column in our live stream containing the AI prediction!
enriched_df = parsed_df.withColumn("sentiment", get_sentiment(F.col("headline")))
watermarked_df = enriched_df.withWatermark("timestamp", "10 minutes")

# --- 5. New Analytics: Sentiment by Source ---
# Let's see which news sources are the most bullish or bearish
sentiment_counts = watermarked_df.groupBy("source", "sentiment").count()

# --- 6. Output Sink ---
query_sentiment = sentiment_counts.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("checkpointLocation", "streaming/checkpoints/sentiment") \
    .option("truncate", False) \
    .start()

spark.streams.awaitAnyTermination()