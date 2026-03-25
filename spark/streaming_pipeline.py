from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# -------------------------------
# Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("RealTimeOrderAnalytics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------
# Read from Kafka (STREAM)
# -------------------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

# -------------------------------
# Schema (MATCH PRODUCER)
# -------------------------------
schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("restaurant_id", StringType()),
    StructField("city", StringType()),
    StructField("order_value", DoubleType()),
    StructField("delivery_time", IntegerType()),
    StructField("payment_type", StringType()),
    StructField("status", StringType())
])

# -------------------------------
# Bronze Layer (parse JSON)
# -------------------------------
bronze_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", current_timestamp())

# -------------------------------
# Silver Layer (cleaning)
# -------------------------------
silver_df = bronze_df \
    .dropDuplicates(["order_id"]) \
    .filter(col("order_value") > 0) \
    .filter(col("status").isNotNull())

# -------------------------------
# Gold Layer (aggregations)
# -------------------------------
revenue_by_city = silver_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        window("event_time", "1 minute"),
        col("city")
    ) \
    .agg(sum("order_value").alias("total_revenue"))

delivery_by_city = silver_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        window("event_time", "1 minute"),
        col("city")
    ) \
    .agg(avg("delivery_time").alias("avg_delivery_time"))

orders_by_restaurant = silver_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        window("event_time", "1 minute"),
        col("city"),
        col("restaurant_id")
    ) \
    .agg(count("*").alias("total_orders"))

# -------------------------------
# Write Streams (CONTINUOUS)
# -------------------------------
revenue_query = revenue_by_city.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/app/data/gold/revenue_by_city") \
    .option("checkpointLocation", "/app/checkpoints/revenue") \
    .start()

delivery_query = delivery_by_city.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/app/data/gold/delivery_by_city") \
    .option("checkpointLocation", "/app/checkpoints/delivery") \
    .start()

orders_query = orders_by_restaurant.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/app/data/gold/orders_by_restaurant") \
    .option("checkpointLocation", "/app/checkpoints/orders") \
    .start()

# -------------------------------
# Keep Stream Alive
# -------------------------------
spark.streams.awaitAnyTermination()