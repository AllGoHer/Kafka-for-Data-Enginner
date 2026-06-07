from kafka import KafkaProducer
import json 

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

producer.send(
    "orders",
    {
        "order_id": 101,
        "status": "CREATED",
        "amount": 250.75
    }
)

producer.flush()
print("JSON event enviado")

