from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer

# Define Kafka Configurations
KAFKA_TOPIC = 'my_topic'
KAFKA_BOOTSTRAP_SERVER = 'localhost:9092'

def produce_to_kafka():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)
    # Produce a message
    message = b'Hello Kafka from Airflow!'
    producer.send(KAFKA_TOPIC, value=message)
    producer.flush()  # Ensure the message is sent before closing the producer
    producer.close()

def consume_from_kafka():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='airflow-consumer-group'
    )
    
    for message in consumer:
        print(f"Consumed message: {message.value.decode('utf-8')}")
        # Break after consuming one message
        break
    consumer.close()

# Airflow DAG
default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG('kafka_producer_consumer_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    produce_task = PythonOperator(
        task_id='produce_to_kafka',
        python_callable=produce_to_kafka
    )

    consume_task = PythonOperator(
        task_id='consume_from_kafka',
        python_callable=consume_from_kafka
    )

    # Define task dependencies
    produce_task >> consume_task
