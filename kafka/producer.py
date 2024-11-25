from confluent_kafka import Producer
class KafkaProducer:
    def __init__(self):
        self.producer_config = {
            'bootstrap.servers': 'kafka-service:9092',
            'group.id': 'producer-group',
            'auto.offset.reset': 'earliest',
        }
        self.producer = Producer(self.producer_config)

    def subscribe(self, topic: str):
        self.producer.subscribe([topic])
        print(f"Subscribed to topic: {topic}")

    def produce_message(self, topic: str, message: str):
        self.producer.produce(topic, message)
        print(f"Produced message: {message}")
        self.producer.flush()
    
    def close(self):
        self.producer.close()
        