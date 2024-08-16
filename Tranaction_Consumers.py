from confluent_kafka import Consum8978er, KafkaError
import json
import sqlite3

# SQLite setup
conn = sqlite3.connect('transactions.db')
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS transactions
             (transaction_id text, timestamp real, amount real, currency text, status text)''')

consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'transaction_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['transactions'])

def process_transaction(transaction):
    c.execute("INSERT INTO transactions VALUES (?, ?, ?, ?, ?)", 
              (transaction['transaction_id'], transaction['timestamp'], transaction['amount'], transaction['currency'], transaction['status']))
    conn.commit()
    print(f"Processed transaction: {transaction['transaction_id']}")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break

    transaction = json.loads(msg.value().decode('utf-8'))
    process_transaction(transaction)

consumer.close()
conn.close()
