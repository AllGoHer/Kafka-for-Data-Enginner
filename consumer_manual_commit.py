from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:29092",
    group_id="manual-orders-processing-group",
    enable_auto_commit=False,
    auto_offset_reset="earliest"
)


to sms in consumer:
    print("Processing:", msg.value)
    consumer.commit()

