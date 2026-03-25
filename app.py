import streamlit as st
import pandas as pd
from streamlit_autorefresh import st_autorefresh

# -------------------------------
# Page Config
# -------------------------------
st.set_page_config(layout="wide")
st.title("🚀 Real-Time Order Analytics Dashboard")

# -------------------------------
# Auto Refresh (every 5 sec)
# -------------------------------
st_autorefresh(interval=5000, key="refresh")

# -------------------------------
# Load Data
# -------------------------------

def safe_read_parquet(path):
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()  # return empty instead of crash

revenue = safe_read_parquet("data/gold/revenue_by_city")
delivery = safe_read_parquet("data/gold/delivery_by_city")
orders = safe_read_parquet("data/gold/orders_by_restaurant")

required_cols = ["city"]

for df in [revenue, delivery, orders]:
    if df.empty or not all(col in df.columns for col in required_cols):
        st.warning("Streaming data loading...")
        st.stop()

# -------------------------------
# Sidebar Filters
# -------------------------------
st.sidebar.header("Filters")

# Combine cities from all datasets (safe approach)
cities = sorted(
    set(revenue['city']) |
    set(delivery['city'])
)

selected_cities = st.sidebar.multiselect(
    "Select City",
    options=cities,
    default=cities
)


# -------------------------------
# Helper Function (Reusable Filter)
# -------------------------------
def apply_city_filter(df):
    return df[df['city'].isin(selected_cities)]

# Apply filters
revenue_filtered = apply_city_filter(revenue)
delivery_filtered = apply_city_filter(delivery)
#orders_filtered = apply_city_filter(orders)

# -------------------------------
# KPIs
# -------------------------------
st.subheader("📊 Key Metrics")

col1, col2, col3 = st.columns(3)

col1.metric(
    "Total Revenue (INR)",
    f"{revenue_filtered['total_revenue'].sum():,.2f}"
)

col2.metric(
    "Avg Delivery Time (mins)",
    f"{delivery_filtered['avg_delivery_time'].mean():.2f}"
)

col3.metric(
    "Total Orders",
    f"{orders['total_orders'].sum()}"
)

# -------------------------------
# Charts
# -------------------------------
st.subheader("📍 Revenue by City")

st.bar_chart(
    revenue_filtered.groupby("city")["total_revenue"].sum()
)

st.subheader("🚚 Avg Delivery Time by City")

st.bar_chart(
    delivery_filtered.groupby("city")["avg_delivery_time"].mean()
)

# -------------------------------
# Top Restaurants
# -------------------------------
st.subheader("🍽 Top Restaurants")

st.dataframe(
    orders
    .sort_values(by="total_orders", ascending=False)
    .head(10),
    width="stretch"
)