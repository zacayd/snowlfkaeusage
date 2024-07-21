import hashlib
import logging
import os
import json
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from sqlalchemy import create_engine, text
from confluent_kafka import Consumer
from utils.logger import Logger
from services.gsp_service import GspService
from services.hive_service import HiveService
from services.kafka_service import KafkaService
from services.hdfs_service import HdfsService


class ConsumerUsage:
    def __init__(self, config):
        self.config = config
        self.log = Logger('Consumer')
        self.hive_service = HiveService(config)
        self.kafka_service = KafkaService(config, self.log)
        self.hdfs_service = HdfsService(config, self.log)

    def generate_md5_hash(self, proc_catalog, proc_schema, proc_name, args):
        combined_string = f"{proc_catalog}:{proc_schema}:{proc_name}:{args}"
        md5_hash = hashlib.md5(combined_string.encode()).hexdigest()
        return md5_hash

    def set_log_location(self, customer_database):
        current_time = datetime.now().strftime("%Y-%m-%d")
        logs_folder = customer_database
        console_handler = logging.StreamHandler(sys.stdout)

        if not os.path.exists(logs_folder):
            os.makedirs(logs_folder)

        self.log.file_name = os.path.join(logs_folder, f"{self.log.logger_name}_{current_time}.log")
        file_handler = logging.FileHandler(self.log.file_name)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.log.logger.addHandler(file_handler)
        self.log.logger.addHandler(console_handler)
        self.log.info("addHandler established!")

    def get_messages_from_topic(self):
        try:
            self.engine = create_engine(self.config['OumDBConnectionString'])
            self.cnxn = self.engine.connect()
            result = self.cnxn.execute(text(self.config['QueryAllCustomers']))
            customers = result.fetchall()
            for customer in customers:
                try:
                    self.process_customer(customer)
                except Exception as e:
                    self.log.error(e)
            self.cnxn.close()
        except Exception as e:
            self.log.error(e)

    def process_customer(self, customer):
        try:
            customer_database = customer[2].split(';')[1].split('=')[1]
            self.set_log_location(customer_database)
            self.log.info(f"Processing customer {customer[1]}")
            # Further processing logic...
        except Exception as e:
            self.log.error(e)

    def process_topic_message(self, comp_name, company_id, connection, customer_database, arango_db_url):
        try:
            arango_db_url = f'http://{arango_db_url}'
            connection_id = connection[3]
            tool_name = connection[0]
            self.hive_service.create_database(customer_database)

            # Further processing logic...
        except Exception as e:
            self.log.error(e)

    # Additional methods...

