from kafka import KafkaConsumer
import os

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:29092",
    group_id="orders-processing-group",
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

for msg in consumer:
    print(f"Consumer {os.getpid()} -> {msg.value}")
