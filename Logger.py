import logging
import os
from datetime import datetime

class Logger:
    def __init__(self, logger_name):
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
        self.loggerName = logger_name

        # Create Logs directory if it doesn't exist
        log_directory = "Logs"
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)

        current_time = datetime.now().strftime("%Y-%m-%d")
        self.file_name = os.path.join(log_directory, f"{logger_name}_{current_time}.log")

        # Create a file handler
        file_handler = logging.FileHandler(self.file_name)
        file_handler.setLevel(logging.DEBUG)  # Set to DEBUG to capture all log levels

        # Create a formatter and set it to the file handler
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        # Create a stream handler
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        # Add the handlers to the logger
        if not self.logger.handlers:
            self.logger.addHandler(file_handler)
            self.logger.addHandler(stream_handler)

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)