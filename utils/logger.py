import logging

class Logger:
    def __init__(self, logger_name):
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.INFO)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)
