# ============================================
# streaming_pipeline.py
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import time

# --------------------------------------------
# 1. Spark Session
# --------------------------------------------

spark = SparkSession.builder \
    .appName("OrderStreamingPipeline") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.metricsEnabled", "false") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------
# 2. Kafka Ingestion
# --------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"
KAFKA_TOPIC = "order_events"

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# --------------------------------------------
# 3. JSON Parsing
# --------------------------------------------

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("event_type", StringType()),
    StructField("quantity", IntegerType()),
    StructField("price", DoubleType()),
    StructField("event_time", StringType())
])

parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), order_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp("event_time"))

clean_df = parsed_df \
    .withWatermark("event_time", "10 minutes") \
    .dropDuplicates(["order_id", "event_time"])

# --------------------------------------------
# 4. Latest State
# --------------------------------------------

def overwrite_latest(batch_df, batch_id):
    latest = batch_df.groupBy("order_id") \
        .agg(max(struct(
            col("event_time"),
            col("customer_id"),
            col("product_id"),
            col("event_type"),
            col("quantity"),
            col("price")
        )).alias("latest")) \
        .select("order_id", "latest.*")

    latest.write.mode("overwrite").parquet("./data/latest_orders")

clean_df.writeStream \
    .foreachBatch(overwrite_latest) \
    .option("checkpointLocation", "./checkpoint/latest") \
    .trigger(processingTime="1 minute") \
    .start()

# --------------------------------------------
# 5. Revenue Window
# --------------------------------------------

order_value_df = clean_df \
    .filter(col("event_type") != "CANCELLED") \
    .withColumn("order_value", col("quantity") * col("price")) \
    .groupBy(window("event_time", "5 minutes"), "customer_id") \
    .sum("order_value") \
    .withColumnRenamed("sum(order_value)", "total_order_value")

order_value_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .partitionBy("customer_id") \
    .option("checkpointLocation", "./checkpoint/order_value") \
    .trigger(processingTime="1 minute") \
    .start("./data/order_value")

# --------------------------------------------
# 6. Cancel Window
# --------------------------------------------

cancel_df = clean_df \
    .filter(col("event_type") == "CANCELLED") \
    .groupBy(window("event_time", "5 minutes")) \
    .count() \
    .withColumnRenamed("count", "cancel_count")

cancel_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", "./checkpoint/cancel") \
    .trigger(processingTime="1 minute") \
    .start("./data/cancel_count")

# --------------------------------------------
# 7. Run for fixed duration
# --------------------------------------------

print("Streaming running for 60 seconds...\n")
time.sleep(60)

print("Stopping streams...\n")
for q in spark.streams.active:
    q.stop()

spark.stop()

print("Streaming completed successfully.")