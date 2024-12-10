import threading
from kafka_service import KafkaConsumer
from hdfs_connection import append_to_olympics_csv
import time
import os
def run_consumer(consumer: KafkaConsumer):
    consumer.consume_messages()


kafka_consumer = KafkaConsumer()
kafka_consumer.subscribe('olympics')
consumer_thread = threading.Thread(target=run_consumer, args=(kafka_consumer,))
consumer_thread.start()

# Function to update the message placeholder
while True:
    print(kafka_consumer.message)
    if len(kafka_consumer.message) > 0:
        append_to_olympics_csv(kafka_consumer.message)
        kafka_consumer.message = ''
        time.sleep(1)