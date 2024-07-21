from confluent_kafka import Consumer
import json

class KafkaService:
    def __init__(self, config, logger):
        self.logger = logger
        self.conf = {
            'bootstrap.servers': config['brokers'],
            'group.id': config['group_id'],
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 600000
        }
        self.consumer = Consumer(self.conf)

    def process_topic(self, topic_name, arango_db_url, customer_database, collection_name):
        self.consumer.subscribe([topic_name])
        while True:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                self.logger.info(f"No messages from kafka topic: {topic_name}....")
                continue
            if msg.error():
                self.logger.error(msg.error())
                continue
            else:
                topic_name = msg.topic()
                list_of_messages = json.loads(msg.value().decode('utf-8'))
                self.logger.info(f"Topic: {topic_name}, Message: {list_of_messages}")
                # Further processing logic...
                self.consumer.commit(msg)
        self.consumer.unsubscribe()
