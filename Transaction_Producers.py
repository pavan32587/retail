from Kafka import KafkaProducer
import random
import time
import uuid
import json

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": time.time(),
        "amount": round(random.uniform(10.0, 1000.0), 2),
        "currency": random.choice(["USD", "EUR", "GBP"]),
        "status": random.choice(["pending", "completed", "failed"])
    }

producer_conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_conf)

while True:
    transaction = generate_transaction()
    producer.produce('transactions', key=transaction['transaction_id'], value=json.dumps(transaction), callback=delivery_report)
    producer.poll(0)
    time.sleep(1)  # Simulate a transaction rate of 1 transaction per second

producer.flush()
