from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    retries=5
)

producer.send("orders", b"order-100")
producer.flush()
print("Mensaje enviado con reintentos activados")

