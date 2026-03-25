# convert_to_csv.py

import pandas as pd

# Revenue
df1 = pd.read_parquet("data/gold/revenue_by_city")
df1.to_csv("data/gold/revenue_by_city.csv", index=False)

# Delivery
df2 = pd.read_parquet("data/gold/delivery_by_city")
df2.to_csv("data/gold/delivery_by_city.csv", index=False)

# Orders
df3 = pd.read_parquet("data/gold/orders_by_restaurant")
df3.to_csv("data/gold/orders_by_restaurant.csv", index=False)

print("✅ CSV files created")