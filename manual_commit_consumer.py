from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:29092",
    group_id="orders_consume_manualoffset",
    enable_auto_commit=False,
    auto_offset_reset="earliest"
)

for msg in consumer:
    print("Processing:", msg.value)
    # simulan el procesamiento exitoso
    consumer.commit()
    print("Offset committed")

