from kafka import KafkaProducer


transaction_configs = {
    "enable.idempotence": True,
    "transactional.id": "my-transaccional-producer"
}

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    acks="all",
    retries=5,               
    **transaction_configs     
)

# OBLIGATORIO para transacciones
producer.init_transactions()

try:
    producer.begin_transaction()
    producer.send("orders", b"orden-101")
    producer.commit_transaction()
    print("Transacción cometida exitosamente (Exactly-Once)")

except Exception as e:
    producer.abort_transaction()
    print("Transaction aborted:", e)

producer.close()
