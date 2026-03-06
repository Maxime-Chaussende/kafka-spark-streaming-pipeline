import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("KafkaSparkSnapshot") \
    .config("spark.sql.shuffle.partitions", "1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("event_time", StringType()) \
    .add("country", StringType()) \
    .add("amount", DoubleType()) \
    .add("event_type", StringType())

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "events") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

agg_df = parsed_df.groupBy("event_type").count()

output_dir = "/opt/output/results"
os.makedirs(output_dir, exist_ok=True)

output_file = os.path.join(output_dir, "events_agg.csv")

rows = agg_df.collect()

with open(output_file, "w", encoding="utf-8", newline="") as f:
    f.write("event_type,count\n")
    for row in rows:
        f.write(f"{row['event_type']},{row['count']}\n")

print(f"Wrote {output_file}", flush=True)

spark.stop()