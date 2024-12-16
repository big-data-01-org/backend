from confluent_kafka import Producer
class KafkaProducer:
    def __init__(self):
        self.producer_config = {
            'bootstrap.servers': 'kafka-service:9092',
        }
        self.producer = Producer(self.producer_config)

    # Topic should be the same topic as the consumer, is subscribed to
    def produce_message(self, topic: str, message: str):
        try:
            self.producer.produce(topic, message)
            print(f"Produced message: {message}")
            self.producer.poll(0)
            self.producer.flush()
        except Exception as e:
            print(f"An error occurred while producing the message: {e}")
            
    
    def close(self):
        self.producer.close()