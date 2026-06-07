from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers=" localhost:29092",
    group_id= "cosume_orders",
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

print("Esperando mensajes en el tópico 'orders'...")
for msg in consumer:
    print(f"Mensaje recibido: {msg.value.decode('utf-8')}")
