from pyspark.sql import SparkSession
from schemas import news_event_schema
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("BatchAnalytics").getOrCreate()

DATA_PATH = "../data/raw/news_validated.jsonl"

df=spark.read.schema(news_event_schema).json(DATA_PATH) 
df.show()

events_per_source=df.groupBy("source").count().orderBy(F.desc("count"))
events_per_source.show()

hourly_events_count=df.withColumn("hour", F.hour("timestamp")).groupBy("hour").count().orderBy("hour")
hourly_events_count.show()

duplicate_events_count=df.groupBy("id").count().filter("count > 1").count()
print(f"Number of duplicate events: {duplicate_events_count}")

top_keywords_in_headlines = df.withColumn("keyword", F.explode(F.split(F.lower("headline"), " "))) \
    .groupBy("keyword").count().orderBy(F.desc("count")).limit(10)

top_keywords_in_headlines.show()
