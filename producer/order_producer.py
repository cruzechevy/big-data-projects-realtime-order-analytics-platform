import json
import time
import random
from kafka import KafkaProducer
from faker import Faker

# Initialize Faker
fake = Faker()

# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Sample data
cities = ["Bangalore", "Mumbai", "Delhi", "Hyderabad"]
payment_types = ["UPI", "Card", "Cash"]

# Function to generate order
def generate_order():
    return {
        "order_id": f"ORD_{random.randint(1000, 9999)}",
        "customer_id": fake.uuid4(),
        "restaurant_id": f"REST_{random.randint(1, 100)}",
        "city": random.choice(cities),
        "order_value": round(random.uniform(100, 1000), 2),
        "delivery_time": random.randint(10, 60),
        "payment_type": random.choice(payment_types),
        "status": "DELIVERED"
    }

# Send data continuously
while True:
    order = generate_order()
    producer.send("orders", value=order)
    print(f"Sent: {order}")
    time.sleep(1)