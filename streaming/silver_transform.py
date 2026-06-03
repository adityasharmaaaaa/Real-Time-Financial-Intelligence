import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Initialize Spark Session with Delta extensions
spark = SparkSession.builder \
    .appName("SilverTransformPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Ensure target directories exist
os.makedirs("lakehouse/silver/news", exist_ok=True)
os.makedirs("lakehouse/silver/news_invalid", exist_ok=True)

# Step 1: Read Bronze Delta Table
bronze_df = spark.read.format("delta").load("lakehouse/bronze/news")

# Step 2: Apply Validation Rules
valid_condition = (
    F.col("headline").isNotNull() & 
    (F.col("headline") != "") & 
    F.col("timestamp").isNotNull() & 
    F.col("source").isNotNull()
)

valid_df = bronze_df.filter(valid_condition)
invalid_df = bronze_df.filter(~valid_condition)

# Step 3: Deduplicate valid records by ID
deduplicated_df = valid_df.dropDuplicates(["id"])

# Step 4: Add Partition Column
final_silver_df = deduplicated_df.withColumn("event_date", F.to_date("timestamp"))

# Write 1: Invalid records to the quarantine/invalid Silver table
invalid_df.write \
    .format("delta") \
    .mode("append") \
    .save("lakehouse/silver/news_invalid")

print(f"Wrote invalid records to lakehouse/silver/news_invalid")

# Write 2: Valid records to the partitioned Silver table
final_silver_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("event_date") \
    .save("lakehouse/silver/news")

print(f"Wrote validated, deduplicated, and partitioned records to lakehouse/silver/news")