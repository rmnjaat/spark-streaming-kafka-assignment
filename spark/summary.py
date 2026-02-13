# ============================================
# summary_report.py
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("StreamingSummaryReport") \
    .getOrCreate()

print("\n================ FINAL STREAMING SUMMARY ================\n")

# ---- Latest Orders ----
latest_df = spark.read.parquet("./data/latest_orders")

total_orders = latest_df.select("order_id").distinct().count()

print("1️⃣ Latest Order State")
print(f"Total Unique Orders: {total_orders}")
latest_df.groupBy("event_type").count().show(truncate=False)

# ---- Revenue ----
order_value_df = spark.read.parquet("./data/order_value")

print("\n2️⃣ Customer Revenue Analytics")
print(f"Total Window Records: {order_value_df.count()}")

top_customer = order_value_df.groupBy("customer_id") \
    .sum("total_order_value") \
    .withColumnRenamed("sum(total_order_value)", "total_revenue") \
    .orderBy(col("total_revenue").desc()) \
    .limit(1)

print("Top Revenue Customer:")
top_customer.show(truncate=False)

# ---- Cancel ----
cancel_df = spark.read.parquet("./data/cancel_count")

total_cancellations = cancel_df.agg(sum("cancel_count")).collect()[0][0]
total_windows = cancel_df.count()

print("\n3️⃣ Cancellation Metrics")
print(f"Total Cancellations: {total_cancellations}")
print(f"Total Windows: {total_windows}")

print("Peak Cancellation Window:")
cancel_df.orderBy(col("cancel_count").desc()).limit(1).show(truncate=False)

print("\n================ END =================\n")

spark.stop()