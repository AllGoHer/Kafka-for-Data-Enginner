from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    acks="all",
    retries=5

)

print("Conectado a Kafka. Enviando mensaje...")
producer.send("orders", b"order_created")
producer.flush()
print("¡Mensaje enviado exitosamente al tópico 'orders'!")
producer.close()

