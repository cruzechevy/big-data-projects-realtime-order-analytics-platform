from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count

spark = SparkSession.builder \
    .appName("GoldOrders") \
    .getOrCreate()

# Read Silver data
df = spark.read.parquet("/app/data/silver/orders")

# 🔹 1. Revenue per city
revenue_city = df.groupBy("city") \
    .agg(sum("order_value").alias("total_revenue"))

# 🔹 2. Avg delivery time per city
delivery_city = df.groupBy("city") \
    .agg(avg("delivery_time").alias("avg_delivery_time"))

# 🔹 3. Orders per restaurant
orders_restaurant = df.groupBy("restaurant_id") \
    .agg(count("*").alias("total_orders"))

# Write outputs
revenue_city.write.mode("overwrite").parquet("/app/data/gold/revenue_by_city")
delivery_city.write.mode("overwrite").parquet("/app/data/gold/delivery_by_city")
orders_restaurant.write.mode("overwrite").parquet("/app/data/gold/orders_by_restaurant")

print("✅ Gold layer created successfully")