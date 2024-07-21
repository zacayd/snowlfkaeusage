
import logging
from datetime import datetime



class Logger:
    def __init__(self, logger_name):
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
        self.loggerName=logger_name

        current_time = datetime.now().strftime("%Y-%m-%d")
        self.file_name = f"{logger_name}_{current_time}.log"

        # create a file handler
        file_handler = logging.FileHandler(self.file_name)
        #file_handler.setLevel(logging.ERROR)

        # create a formatter and set it to the file handler
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s' )
        # file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)


        # # add the file handler to the logger
        # self.logger.addHandler(file_handler)

        if not self.logger.handlers:
            # self.logger.addHandler(console_handler)
            self.logger.addHandler(file_handler)
            self.logger.addHandler(stream_handler)
        #     # self.logger.addHandler(handlerRotator)

        # add the file handler to the screen




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