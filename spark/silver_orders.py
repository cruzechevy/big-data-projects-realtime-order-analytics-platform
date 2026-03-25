from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("SilverOrders") \
    .getOrCreate()

# Read Bronze data
bronze_df = spark.read.parquet("/app/data/bronze/orders")

# 🔹 Basic Cleaning
silver_df = bronze_df \
    .dropDuplicates(["order_id"]) \
    .filter(col("order_value") > 0) \
    .filter(col("status").isNotNull())

# 🔹 Optional: select only required columns
silver_df = silver_df.select(
    "order_id",
    "customer_id",
    "restaurant_id",
    "city",
    "order_value",
    "delivery_time",
    "payment_type",
    "status"
)

# Write to Silver layer
silver_df.write \
    .mode("overwrite") \
    .parquet("/app/data/silver/orders")

print("✅ Silver layer created successfully")