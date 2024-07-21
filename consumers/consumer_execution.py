from consumers.consumer_usage import ConsumerUsage
from utils.logger import Logger

class ConsumerExecution:
    def __init__(self, config):
        self.config = config
        self.logger = Logger('ConsumerExecution')

        try:
            self.consumer = ConsumerUsage(config)
        except Exception as e:
            self.logger.error(e)

    def run(self):
        self.logger.info("Start..")
        try:
            self.consumer.get_messages_from_topic()
        except Exception as e:
            self.logger.error(e)

