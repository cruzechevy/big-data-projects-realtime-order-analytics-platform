from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

print("bronze job")

spark = SparkSession.builder \
    .appName("BronzeOrders") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

#2 Define the structure of KAFKA JSON

schema = StructType() \
        .add("order_id", StringType()) \
        .add("customer_id", StringType()) \
        .add("restaurant_id", StringType()) \
        .add("city", StringType()) \
        .add("order_value", DoubleType()) \
        .add("delivery_time", IntegerType()) \
        .add("payment_type", StringType()) \
        .add("status", StringType())

#3 Read from KAFKA
kafka_df =spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers","kafka:9092") \
        .option("subscribe","orders") \
        .option("startingOffsets","latest") \
        .load()

# 4. Convert Kafka value (binary → string)
json_df = kafka_df.selectExpr("CAST(value AS STRING)")

# 5. Parse JSON into columns
parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# 6. Write stream to console (Bronze layer simulation)
# query = parsed_df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start()

query = parsed_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "/app/data/bronze/orders") \
    .option("checkpointLocation", "/app/checkpoints/bronze/orders") \
    .start()

query.awaitTermination()
