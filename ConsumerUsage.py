
import hashlib
import logging
import os
import sys
import time
import json
import requests
from datetime import datetime
from sqlalchemy import create_engine, text
from confluent_kafka import Consumer
import pandas as pd
import xml.etree.ElementTree as ET
from pyhive import hive
from hdfs import InsecureClient
from concurrent.futures import ThreadPoolExecutor, as_completed


from Logger import Logger


class ConsumerUsage:
    def __init__(self, groupid, brokers, sqlConnectionString, arangoDBUrl, listOfTables, dbvVendor, gspUrl, hiveServer, hivePort, hdfs_host, hdfs_port,data):
        self.brokers = brokers
        self.group_id = groupid
        self.sqlConnectionString = sqlConnectionString
        self.arangoDBUrl = arangoDBUrl
        self.listOfTables = listOfTables
        self.hive_server = hiveServer
        self.hive_port = hivePort
        self.log = Logger('Consumer')
        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port
        self.configJsonDict=data
        self.conf = {
            'bootstrap.servers': brokers,
            'group.id': "123",
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 600000
        }
        self.consumer = Consumer(self.conf)
        self.dbvVendor = dbvVendor
        self.gspUrl = gspUrl

    def generate_md5_hash(self, proc_catalog, proc_schema, proc_name, args):
        combined_string = f"{proc_catalog}:{proc_schema}:{proc_name}:{args}"
        md5_hash = hashlib.md5(combined_string.encode()).hexdigest()
        return md5_hash

    def generate_md5_hashTxt(self, combined_string):
        md5_hash = hashlib.md5(combined_string.encode()).hexdigest()
        return md5_hash

    def setLogLocation(self, CustomerDatabase):
        CompName = CustomerDatabase
        current_time = datetime.now().strftime("%Y-%m-%d")
        logs_folder = CompName
        console_handler = logging.StreamHandler(sys.stdout)

        if not os.path.exists(logs_folder):
            try:
                os.makedirs(logs_folder)
                self.log.file_name = os.path.join(logs_folder, f"{self.log.loggerName}_{current_time}.log")
                file_handler = logging.FileHandler(self.log.file_name)
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                file_handler.setFormatter(formatter)
                console_handler.setFormatter(formatter)
                self.log.logger.addHandler(file_handler)
                self.log.logger.addHandler(console_handler)
                self.log.info("addHandler established!")
            except Exception as e:
                self.log.error(e)
        else:
            self.log.file_name = os.path.join(logs_folder, f"{self.log.loggerName}_{current_time}.log")
            file_handler = logging.FileHandler(self.log.file_name)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            self.log.logger.handlers = [file_handler, console_handler]

        return CompName

    def GetMessagesFromTopic(self):
        OumconnectionString = self.sqlConnectionString
        self.server = OumconnectionString.split(';')[0].split('=')[1]
        self.database = OumconnectionString.split(';')[1].split('=')[1]
        self.usr = OumconnectionString.split(';')[2].split('=')[1]
        self.pwd = OumconnectionString.split(';')[3].split('=')[1]

        self.pwd = self.pwd.replace('@', '%40')
        self.port = "1433"

        try:
            self.engine = create_engine(
                f"mssql+pyodbc://{self.usr}:{self.pwd}@{self.server}:{self.port}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes")
            self.cnxn = self.engine.connect()
            result = self.cnxn.execute(text(self.configJsonDict['QueryAllCustomers']))
            customers = result.fetchall()
            for customer in customers:
                try:
                      self.ProcessCustomer(customer)
                except Exception as e:
                    self.log.error(e)
        except Exception as e:
            self.log.error(e)

    def ProcessCustomer(self, customer):
        try:
            CompName = customer[1]
            arangoDBUrl = self.arangoDBUrl
            CustomerConnectionString = customer[2]
            CompanyId = customer[0]
            list_of_tables = self.listOfTables

            CustomerServer = CustomerConnectionString.split(';')[0].split('=')[1]
            CustomerDatabase = CustomerConnectionString.split(';')[1].split('=')[1]
            CustomerUsr = CustomerConnectionString.split(';')[2].split('=')[1]
            CustomerPwd = CustomerConnectionString.split(';')[3].split('=')[1]
            CustomerPwd = CustomerPwd.replace('@', '%40')
            customerEngine = create_engine(
                f"mssql+pyodbc://{CustomerUsr}:{CustomerPwd}@{CustomerServer}:{self.port}/{CustomerDatabase}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes")
            customerCnx = customerEngine.connect()
            connresult = customerCnx.execute(text(self.configJsonDict['ToolQuery']))
            connections = connresult.fetchall()

            self.setLogLocation(CustomerDatabase)
        except Exception as e:
            self.log.error(e)

        if len(connections) > 0:
            for connection in connections:
                 self.ProcessTopicMessage(CompName, CompanyId, connection, CustomerDatabase, arangoDBUrl)

        try:
            customerCnx.close()
        except Exception as e:
            self.log.error(e)

    def constructDoc(self, message, url, db_name):
        try:
            result_users = self.GetMessageFromCollection(db_name, message, url, "users", message["user_id"])
            if result_users:
                result_views = self.GetMessageFromCollection(db_name, result_users, url, "views", message["view_id"])
            if result_views:
                result_workbooks = self.GetMessageFromCollection(db_name, result_views, url, "system_users", result_views["users"][0]["system_user_id"])
            if result_workbooks:
                result_projects = self.GetMessageFromCollection(db_name, result_workbooks, url, "workbooks", result_workbooks["views"][0]["workbook_id"])
            if result_projects:
                resultFinal = self.GetMessageFromCollection(db_name, result_projects, url, "projects", result_projects["workbooks"][0]["project_id"])
        except Exception as e:
            self.log.error(e)
        return resultFinal

    def GetMessageFromCollection(self, db_name, message, url, collection_name, id):
        try:
            aql_query = f'FOR doc IN {collection_name} FILTER doc.id == {id} RETURN doc'
            headers = {'Content-Type': 'application/json'}
            request_url = f'{url}/_db/{db_name}/_api/cursor'
            payload = {
                'query': aql_query,
                'count': True
            }
            response = requests.post(request_url, headers=headers, data=json.dumps(payload))
            if response.status_code == 201:
                result = response.json()
                keys = result['result']
                message[collection_name] = keys
                return message
            else:
                return {}
        except Exception as e:
            self.log.error(e)

    def ProcessTopicMessage(self, CompName, CompanyId, connection, CustomerDatabase, arangoDBUrl):
        try:
            arangoDBUrl = f'http://{arangoDBUrl}'
            connectionID = connection[3]
            ToolName = connection[0]
            self.create_database(arangoDBUrl, CustomerDatabase)

            list_of_tables = self.listOfTables

            self.run(arangoDBUrl, CustomerDatabase, CompanyId, CompName, connectionID, ToolName, list_of_tables)
        except Exception as e:
            self.log.error(e)

    # def  process_topic(self, topic_name, arangoDBUrl, CustomerDatabase, collectionName):
    #     self.consumer.subscribe([topic_name])
    #     toolName = topic_name.split('_')[3]
    #     isDone = True
    #     start_time = time.time()
    #     while not self.done_with_topic(start_time, collectionName) or isDone:
    #         msg = self.consumer.poll(timeout=1.0)
    #         if msg is None:
    #             self.log.info(f"No messages from kafka topic: {topic_name}....")
    #             isDone = False
    #             continue
    #         if msg.error():
    #             self.log.error(msg.error())
    #             continue
    #         else:
    #             topic_name = msg.topic()
    #             listOfMessages = json.loads(msg.value().decode('utf-8'))
    #             self.log.info(f"Topic: {topic_name}, Message: {listOfMessages}")
    #             collectionName = topic_name.split('_TOPIC_')[-1]
    #             if collectionName == "http_requests":
    #                 try:
    #                     if not (listOfMessages["action"] == 'bootstrapSession' and listOfMessages['currentsheet'] != ''):
    #                         listOfMessages = []
    #                     else:
    #                         originalPart = listOfMessages['currentsheet']
    #                         split_parts = originalPart.split('/')
    #                         split_parts.insert(-1, "sheets")
    #                         new_string = '/'.join(split_parts)
    #                         listOfMessages['currentsheet'] = new_string
    #                 except Exception as e:
    #                     self.log.error(e)
    #             if collectionName == "historical_events":
    #                 if listOfMessages["hist_view_id"] is None:
    #                     listOfMessages = []
    #
    #             if listOfMessages:
    #                 if toolName == "SNOWFLAKE":
    #                     if collectionName == "QUERY_HISTORY":
    #                         self.insert_document(arangoDBUrl, CustomerDatabase, collectionName, listOfMessages, str(listOfMessages["QUERY_ID"]), toolName)
    #                 elif toolName == 'SQLS':
    #                     if collectionName == "QUERY_HISTORY":
    #                         self.insert_document(arangoDBUrl, CustomerDatabase, collectionName, listOfMessages, str(listOfMessages["object_id"]), toolName)
    #                     elif   collectionName in ["VIEWS", "TABLES"]:
    #                         self.insert_document(arangoDBUrl, CustomerDatabase, collectionName, listOfMessages, str(listOfMessages["object_id"]), toolName)
    #                 elif collectionName in ["VIEWS", "TABLES"]:
    #                     self.insert_document(arangoDBUrl, CustomerDatabase, collectionName, listOfMessages, str(listOfMessages["TABLE_ID"]), toolName)
    #                 elif collectionName in ["COLUMNS"]:
    #                     self.insert_document(arangoDBUrl, CustomerDatabase, collectionName, listOfMessages, str(listOfMessages["COLUMN_ID"]), toolName)
    #                 elif collectionName in ["PROCEDURES"]:
    #                     procCatalog = listOfMessages["PROCEDURE_CATALOG"]
    #                     procSchema = listOfMessages["PROCEDURE_SCHEMA"]
    #                     procName = listOfMessages["PROCEDURE_NAME"]
    #                     args = listOfMessages["ARGUMENT_SIGNATURE"]
    #                     id = self.generate_md5_hash(procCatalog, procSchema, procName, args)
    #                     self.insert_document(arangoDBUrl, CustomerDatabase, collectionName, listOfMessages, id, toolName)
    #                 elif collectionName == "http_requests":
    #                     AQL = self.configJsonDict["AQL"]
    #                     if listOfMessages:
    #                         currentsheet = listOfMessages["currentsheet"]
    #                         query = {
    #                             "query": AQL,
    #                             "bindVars": {"currentsheet": currentsheet}
    #                         }
    #                         self.save(arangoDBUrl, CustomerDatabase, 'tableau_usage', query, 'repositoryUrl')
    #                 elif collectionName == "historical_events":
    #                     AQL = self.configJsonDict["AQL_AUDIT"]
    #                     if listOfMessages:
    #                         id = listOfMessages["id"]
    #                         query = {
    #                             "query": AQL,
    #                             "bindVars": {"id": id}
    #                         }
    #                         self.save(arangoDBUrl, CustomerDatabase, 'tableau_audit', query, "id")
    #
    #         self.consumer.commit(msg)
    #
    #     self.consumer.unsubscribe()

    def process_topic(self, topic_name, arangoDBUrl, CustomerDatabase, collectionName):
        conf = {
            'bootstrap.servers': self.brokers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 600000
        }
        consumer = Consumer(conf)
        consumer.subscribe([topic_name])
        toolName = topic_name.split('_')[3]
        isDone = True
        start_time = time.time()
        while not self.done_with_topic(start_time, collectionName) or isDone:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                self.log.info(f"No messages from kafka topic: {topic_name}....")
                isDone = False
                continue
            if msg.error():
                self.log.error(msg.error())
                continue
            else:
                topic_name = msg.topic()
                listOfMessages = json.loads(msg.value().decode('utf-8'))
                self.log.info(f"Topic: {topic_name}, Message: {listOfMessages}")
                collectionName = topic_name.split('_TOPIC_')[-1]
                if collectionName == "http_requests":
                    try:
                        if not (listOfMessages["action"] == 'bootstrapSession' and listOfMessages[
                            'currentsheet'] != ''):
                            listOfMessages = []
                        else:
                            originalPart = listOfMessages['currentsheet']
                            split_parts = originalPart.split('/')
                            split_parts.insert(-1, "sheets")
                            new_string = '/'.join(split_parts)
                            listOfMessages['currentsheet'] = new_string
                    except Exception as e:
                        self.log.error(e)
                if collectionName == "historical_events":
                    if listOfMessages["hist_view_id"] is None:
                        listOfMessages = []

                if listOfMessages:
                    if toolName == "SNOWFLAKE":
                        if collectionName == "QUERY_HISTORY":
                            self.insert_document(arangoDBUrl, CustomerDatabase, collectionName, listOfMessages,
                                                 str(listOfMessages["QUERY_ID"]), toolName)
                    elif toolName == 'SQLS':
                        if collectionName == "QUERY_HISTORY":
                            self.insert_document(arangoDBUrl, CustomerDatabase, collectionName, listOfMessages,
                                                 str(listOfMessages["object_id"]), toolName)
                        elif collectionName in ["VIEWS", "TABLES"]:
                            self.insert_document(arangoDBUrl, CustomerDatabase, collectionName, listOfMessages,
                                                 str(listOfMessages["object_id"]), toolName)
                    elif collectionName in ["VIEWS", "TABLES"]:
                        self.insert_document(arangoDBUrl, CustomerDatabase, collectionName, listOfMessages,
                                             str(listOfMessages["TABLE_ID"]), toolName)
                    elif collectionName in ["COLUMNS"]:
                        self.insert_document(arangoDBUrl, CustomerDatabase, collectionName, listOfMessages,
                                             str(listOfMessages["COLUMN_ID"]), toolName)
                    elif collectionName in ["PROCEDURES"]:
                        procCatalog = listOfMessages["PROCEDURE_CATALOG"]
                        procSchema = listOfMessages["PROCEDURE_SCHEMA"]
                        procName = listOfMessages["PROCEDURE_NAME"]
                        args = listOfMessages["ARGUMENT_SIGNATURE"]
                        id = self.generate_md5_hash(procCatalog, procSchema, procName, args)
                        self.insert_document(arangoDBUrl, CustomerDatabase, collectionName, listOfMessages, id,
                                             toolName)
                    elif collectionName == "http_requests":
                        AQL = self.configJsonDict["AQL"]
                        if listOfMessages:
                            currentsheet = listOfMessages["currentsheet"]
                            query = {
                                "query": AQL,
                                "bindVars": {"currentsheet": currentsheet}
                            }
                            self.save(arangoDBUrl, CustomerDatabase, 'tableau_usage', query, 'repositoryUrl')
                    elif collectionName == "historical_events":
                        AQL = self.configJsonDict["AQL_AUDIT"]
                        if listOfMessages:
                            id = listOfMessages["id"]
                            query = {
                                "query": AQL,
                                "bindVars": {"id": id}
                            }
                            self.save(arangoDBUrl, CustomerDatabase, 'tableau_audit', query, "id")

            consumer.commit(msg)

        consumer.unsubscribe()
        consumer.close()

    def done_with_topic(self, start_time, collectionName):
        with open('Config.json', 'r') as f:
            config = json.load(f)
        duration = self.configJsonDict["timeToWait"][collectionName]
        current_time = time.time()
        if current_time - start_time > duration:
            return True
        return False

    def run(self, arangoDBUrl, CustomerDatabase, CompanyId, CompName, connectionID, ToolName, list_of_tables):
        while True:
            try:
                topics = [f'{CompanyId}_{CompName}_{connectionID}_{ToolName}_TOPIC_{tableName}' for tableName in
                          list_of_tables]
                with ThreadPoolExecutor(max_workers=len(topics)) as executor:
                    futures = [
                        executor.submit(self.process_topic, topic.upper(), arangoDBUrl, CustomerDatabase,
                                        topic.split('_TOPIC_')[-1].upper())
                        for topic in topics
                    ]
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as ex:
                            self.log.error(ex)
            except Exception as ex:
                self.log.error(ex)


    def HandleViewsAuditing(self, CustomerDatabase, arangoDBUrl, collectionName, listOfMessages):
        createdAt = listOfMessages["created_at"]
        updatedAt = listOfMessages["updated_at"]
        later_date = max(createdAt, updatedAt)
        auditID = str(listOfMessages['id']) + '_' + str(later_date)
        md5_hash = hashlib.md5(auditID.encode()).hexdigest()
        with open('Config.json', 'r') as f:
            config = json.load(f)

        collectionName = 'tableau_audit'
        AQL = config["AQL_AUDIT"]
        view_id_value = listOfMessages["id"]

        query = {
            "query": AQL,
            "bindVars": {"view_id": view_id_value}
        }

        self.create_collection(arangoDBUrl, CustomerDatabase, collectionName)

        try:
            url = f'{arangoDBUrl}/_db/{CustomerDatabase}/_api/cursor'
            headers = {
                'Content-Type': 'application/json'
            }
            payload = json.dumps(query)
            response = requests.post(url, data=payload, headers=headers)
            if response.status_code == 201:
                results = response.json()['result']
                for document in results:
                    document['auditID'] = md5_hash
                    self.insert_document(arangoDBUrl, CustomerDatabase, collectionName, document, md5_hash)
        except Exception as e:
            self.log.error(e)

    def create_collection(self, arango_url, database_name, collection_name):
        url = f'{arango_url}/_db/{database_name}/_api/collection/{collection_name}'

        response = requests.get(url)
        if response.status_code == 404:
            collection_payload = {
                'name': collection_name,
                'type': 2
            }
            response = requests.post(f'{arango_url}/_db/{database_name}/_api/collection', json=collection_payload)
            if response.status_code == 200:
                self.log.info(f"The collection '{collection_name}' was created.")
            else:
                self.log.error(f"Error creating the collection:{response.text}")
        else:
            self.log.info(f"The collection '{collection_name}' already exists.")

    def create_hive(self, hive_server, hive_port, database_name):
        try:
            conn = hive.Connection(host=hive_server, port=hive_port, username='hive')
            cursor = conn.cursor()
            create_db_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
            cursor.execute(create_db_query)
            self.log.info(f"Database {database_name} is ready")
        except Exception as e:
            self.log.error(e)

    def checkIfexists(self, url, db_name, collection_name, keyCheck):
        aql_query = f"FOR doc IN {collection_name} FILTER doc.id == '{keyCheck}' RETURN doc"
        headers = {'Content-Type': 'application/json'}
        request_url = f'{url}/_db/{db_name}/_api/cursor'
        payload = {
            'query': aql_query,
            'count': True
        }
        response = requests.post(request_url, headers=headers, data=json.dumps(payload))
        if response.status_code == 201:
            result = response.json()
            keys = result['result']
            if keys:
                return True
            else:
                return False
        else:
            self.log.error(f"An error occurred: {response.text}")
            return False

    def put_document(self, arango_url, database_name, collection_name, json_document, keyToCheck, toolName):
        try:
            isExists = self.checkIfexists(arango_url, database_name, collection_name, keyToCheck)
            if not isExists:
                url = f'{arango_url}/_db/{database_name}/_api/document/{collection_name}'
                headers = {'Content-Type': 'application/json'}
                data = json.loads(json_document)
                data["ToolName"] = toolName
                if toolName == 'SNOWFLAKE':
                    self.HandleSnowflake(collection_name, data, database_name, headers, url)
                if toolName == 'SQLS':
                    self.HandleSqls(collection_name, data, database_name, headers, url)
            else:
                self.log.info("Document already exists")
        except Exception as e:
            self.log.error(e)

    def HandleSnowflake(self, collection_name, data, database_name, headers, url):
        if data['QUERY_TEXT'] != "<redacted>":
            txt = data['QUERY_TEXT']
            xml_data = self.GspParsing(txt)
            root = ET.fromstring(xml_data)
            xml_dict = self.parse_element(root)
            json_data = json.dumps(xml_dict, indent=4)
            json_dict = json.loads(json_data)
            if json_dict:
                if 'error' not in json_dict:
                    if "table" in json_dict:
                        if isinstance(json_dict["table"], list):
                            for table in json_dict["table"]:
                                for col in table["column"]:
                                    data["db"] = table["database"] if table["database"] else data["DATABASE_NAME"]
                                    data["schemaName"] = table["schema"] if table["schema"] else data["SCHEMA_NAME"]
                                    data["tablename"] = table["name"].split(".")[-1],
                                    data["columnName"] = col["name"]
                                    timestamp_ms = data["START_TIME"]
                                    data["START_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                    data["START_TIME_"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                    if data["eventname"] == "sql_batch_completed":
                                        data["END_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                    else:
                                        data["END_TIME"] = None
                                    payload = json.dumps(data)
                                    response = requests.post(url, headers=headers, data=payload)
                                    try:
                                        self.json_to_parquet_to_hive(payload, collection_name, database_name, "START_TIME", is_file=True)
                                    except Exception as e:
                                        self.log.error(e)
                                    response = requests.post(url, headers=headers, data=payload)
                                    if response.status_code in [201, 202]:
                                        self.log.info("Document put successfully.")
                                    else:
                                        self.log.error("Error putting document.")
                        else:
                            if isinstance(json_dict["table"]["column"], list):
                                for col in json_dict["table"]["column"]:
                                    data["db"] = json_dict["table"]["database"] if json_dict["table"]["database"] else data["DATABASE_NAME"]
                                    data["schemaName"] = json_dict["table"]["schema"] if json_dict["table"]["schema"] else data["SCHEMA_NAME"]
                                    data["tablename"] = json_dict["table"]["name"].split(".")[-1]
                                    data["columnName"] = col["name"]
                                    timestamp_ms = data["START_TIME"]
                                    data["START_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                    timestamp_ms = data["END_TIME"]
                                    data["END_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                    payload = json.dumps(data)
                                    response = requests.post(url, headers=headers, data=payload)
                                    try:
                                        self.json_to_parquet_to_hive(payload, collection_name, database_name, "START_TIME", is_file=True)
                                    except Exception as e:
                                        self.log.error(e)
                                    if response.status_code in [201, 202]:
                                        self.log.info("Document put successfully.")
                                    else:
                                        self.log.error("Error putting document.")
                            else:
                                data["db"] = json_dict["table"]["database"] if json_dict["table"]["database"] else data["DATABASE_NAME"]
                                data["schemaName"] = json_dict["table"]["schema"] if json_dict["table"]["schema"] else data["SCHEMA_NAME"]
                                data["tablename"] = json_dict["table"]["name"].split(".")[-1]
                                data["columnName"] = json_dict["table"]["column"]["name"]
                                timestamp_ms = data["START_TIME"]
                                data["START_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                timestamp_ms = data["END_TIME"]
                                data["END_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                payload = json.dumps(data)
                                response = requests.post(url, headers=headers, data=payload)
                                try:
                                    self.json_to_parquet_to_hive(payload, collection_name, database_name, "START_TIME", is_file=True)
                                except Exception as e:
                                    self.log.error(e)
                                if response.status_code in [201, 202]:
                                    self.log.info("Document put successfully.")
                                else:
                                    self.log.error("Error putting document.")
                    else:
                        timestamp_ms = data["START_TIME"]
                        data["START_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)
                        timestamp_ms = data["END_TIME"]
                        data["END_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)
                        payload = json.dumps(data)
                        response = requests.post(url, headers=headers, data=payload)
                        if response.status_code in [201, 202]:
                            self.log.info("Document put successfully.")
                        else:
                            self.log.error("Error putting document.")
                else:
                    payload = json.dumps(data)
                    response = requests.post(url, headers=headers, data=payload)
                    if response.status_code in [201, 202]:
                        self.log.info("Document put successfully.")
                    else:
                        self.log.error("Error putting document.")

    def HandleSqls(self, collection_name, data, database_name, headers, url):
        try:
            if data['batchtext'] != "<redacted>":
                txt = data['batchtext']
                # if "Employee" in text:
                #     print('HI')

                xml_data = self.GspParsing(txt)
                root = ET.fromstring(xml_data)
                xml_dict = self.parse_element(root)
                json_data = json.dumps(xml_dict, indent=4)
                json_dict = json.loads(json_data)
                if json_dict:
                        if "table" in json_dict:
                            if isinstance(json_dict["table"], list):
                                for table in json_dict["table"]:
                                    for col in table["column"]:
                                        if isinstance(col, list):
                                            for clm in col:
                                                data["db"] = table["database"].replace("[", "").replace("]", "") if "database" in table else data["databasename"]
                                                data["schemaName"] = table["schema"].replace("[", "").replace("]", "") if "schema" in table else 'dbo'
                                                data["tablename"] = table["name"].split(".")[-1].replace("[", "").replace("]", "")
                                                data["columnName"] = clm["name"]
                                                timestamp_ms = data["event_time"]
                                                data["event_time"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                                data["event_time_"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                                data["START_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                                data["START_TIME_"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                                if data["eventname"] == "sql_batch_completed":
                                                    data["END_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                                else:
                                                    data["END_TIME"] = None
                                                data["QUERY_ID"] = self.generate_md5_hashTxt(f'{data["object_id"]}{data["event_time"]}')
                                                payload = json.dumps(data)
                                                response = requests.post(url, headers=headers, data=payload)
                                                try:
                                                    self.json_to_parquet_to_hive(payload, collection_name, database_name, "START_TIME", is_file=True)
                                                except Exception as e:
                                                    self.log.error(str(e))
                                                if response.status_code in [201, 202]:
                                                    self.log.info("Document put successfully.")
                                                else:
                                                    self.log.error(f"Error putting document: {response.text}")
                                        else:
                                            data["db"] = table["database"] if "database" in table else data["databasename"]
                                            data["schemaName"] = table["schema"] if "schema" in table else 'dbo'
                                            data["tablename"] = str(table["name"].split(".")[-1])
                                            data["columnName"] = col["name"]
                                            timestamp_ms = data["event_time"]
                                            data["event_time"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                            data["event_time_"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                            data["START_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                            data["START_TIME_"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                            if data["eventname"] == "sql_batch_completed":
                                                data["END_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                            else:
                                                data["END_TIME"] = None
                                            data["QUERY_ID"] = self.generate_md5_hashTxt(f'{data["object_id"]}{data["event_time"]}')
                                            payload = json.dumps(data)
                                            response = requests.post(url, headers=headers, data=payload)
                                            try:
                                                self.json_to_parquet_to_hive(payload, collection_name, database_name, "START_TIME", is_file=True)
                                            except Exception as e:
                                                self.log.error(str(e))
                                            if response.status_code in [201, 202]:
                                                self.log.info("Document put successfully.")
                                            else:
                                                self.log.error("Error putting document: %s", response.text)
                            else:
                                if isinstance(json_dict["table"]["column"], list):
                                    for col in json_dict["table"]["column"]:
                                        data["db"] = json_dict["table"]["database"] if "database" in json_dict["table"] else data["databasename"]
                                        data["schemaName"] = json_dict["table"]["schema"] if "schema" in json_dict["table"] else 'dbo'
                                        data["tablename"] = json_dict["table"]["name"].split(".")[-1]
                                        data["columnName"] = col["name"]
                                        timestamp_ms = data["event_time"]
                                        data["event_time"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                        data["event_time_"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                        data["START_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                        data["START_TIME_"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                        if data["eventname"] == "sql_batch_completed":
                                            data["END_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                        else:
                                            data["END_TIME"] = None
                                        data["QUERY_ID"] = self.generate_md5_hashTxt(f'{data["object_id"]}{data["event_time"]}')
                                        payload = json.dumps(data)
                                        response = requests.post(url, headers=headers, data=payload)
                                        try:
                                            self.json_to_parquet_to_hive(payload, collection_name, database_name, "START_TIME", is_file=True)
                                        except Exception as e:
                                            self.log.error(str(e))
                                        if response.status_code in [201, 202]:
                                            self.log.info("Document put successfully.")
                                        else:
                                            self.log.error("Error putting document: %s", response.text)
                                else:
                                    data["db"] = json_dict["table"]["database"] if "database" in json_dict["table"] and json_dict["table"]["database"] else data["databasename"]
                                    data["schemaName"] = json_dict["table"]["schema"] if "schema" in json_dict["table"] else 'dbo'
                                    data["tablename"] = json_dict["table"]["name"].split(".")[-1]
                                    data["columnName"] = json_dict["table"]["column"]["name"]
                                    timestamp_ms = data["event_time"]
                                    data["event_time"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                    data["event_time_"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                    data["START_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                    data["START_TIME_"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                    if data["eventname"] == "sql_batch_completed":
                                        data["END_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                    else:
                                        data["END_TIME"] = None
                                    try:
                                        data["QUERY_ID"] = self.generate_md5_hashTxt(f'{data["object_id"]}{data["event_time"]}')
                                    except Exception as e:
                                        self.log.error(str(e))
                                    payload = json.dumps(data)
                                    response = requests.post(url, headers=headers, data=payload)
                                    try:
                                        self.json_to_parquet_to_hive(payload, collection_name, database_name,
                                                                     "START_TIME", is_file=True)
                                    except Exception as e:
                                        self.log.error(str(e))
                                    if response.status_code in [201, 202]:
                                        self.log.info("Document put successfully.")
                                    else:
                                        self.log.error("Error putting document: %s", response.text)
                        else:
                            payload = json.dumps(data)
                            response = requests.post(url, headers=headers, data=payload)
                            if response.status_code in [201, 202]:
                                self.log.info("Document put successfully.")
                            else:
                                self.log.error("Error putting document: %s", response.text)
        except Exception as e:
            self.log.error(str(e))

    def ConvertLinuxToDateTime(self, timestamp_ms):
        if not isinstance(timestamp_ms, (int, float)):
            self.log.info(f"The timestamp is not a number: {timestamp_ms}")
            return timestamp_ms
        try:
            timestamp_s = timestamp_ms / 1000.0
            date_time = datetime.fromtimestamp(timestamp_s)
            date_string = date_time.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            self.log.error(e)
            return None
        return date_string

    def update_document(self, arango_url, database_name, collection_name, json_document, keyToCheck):
        try:
            isExists = self.checkIfexists(arango_url, database_name, collection_name, keyToCheck)
            if not isExists:
                url = f'{arango_url}/_db/{database_name}/_api/document/{collection_name}'
                headers = {'Content-Type': 'application/json'}
                data = json.loads(json_document)
                payload = json.dumps(data)
                response = requests.post(url, headers=headers, data=payload)
                if response.status_code in [201, 202]:
                    self.log.info("Document put successfully.")
                else:
                    self.log.error("Error putting document.")
            else:
                self.log.info("Document already exists")
        except Exception as e:
            self.log.error(e)

    def parse_element(self, element):
        parsed_data = {}
        if element.attrib:
            parsed_data.update(element.attrib)
        if element.text and element.text.strip():
            parsed_data["text"] = element.text.strip()
        for child in element:
            child_parsed = self.parse_element(child)
            child_tag = child.tag
            if child_tag not in parsed_data:
                parsed_data[child_tag] = child_parsed
            else:
                if not isinstance(parsed_data[child_tag], list):
                    parsed_data[child_tag] = [parsed_data[child_tag]]
                parsed_data[child_tag].append(child_parsed)
        return parsed_data

    def create_database(self, arango_url, database_name):
        url = f'{arango_url}/_api/database'
        data = {'name': database_name}
        response = requests.post(url, json=data)
        if response.status_code == 201:
            self.log.info(f"The database '{database_name}' was created.")
        else:
            self.log.info(f"The database {database_name} exits!")

    def GspParsing(self, query):
        payload = json.dumps({"query": query, "vendor": self.dbvVendor})
        headers = {'Content-Type': 'application/json'}
        response = requests.request("POST", self.gspUrl, headers=headers, data=payload)
        return response.text

    def insert_document(self, arango_url, database_name, collection_name, json_document, keyToCheck, toolName):
        self.create_collection(arango_url, database_name, collection_name)
        self.create_hive(self.hive_server, self.hive_port, database_name)
        jObject = json.dumps(json_document)
        try:
            self.put_document(arango_url, database_name, collection_name, jObject, keyToCheck, toolName)
        except Exception as e:
            self.log.error(e)

    def json_to_parquet_to_hive(self, json_input, table_name, database_name, dateField, is_file=True):
        parquet_dir = f'{database_name}/{table_name}'
        if not os.path.exists(parquet_dir):
            os.makedirs(parquet_dir)
        if is_file:
            try:
                json_dict = json.loads(json_input)
                json_dict["tablename"] = json_dict["tablename"]
                df = pd.DataFrame([json_dict])
                df['START_TIME'] = pd.to_datetime(df['START_TIME'])
                df['END_TIME'] = pd.to_datetime(df['END_TIME'])
            except Exception as e:
                self.log.error(e)
        else:
            df = pd.read_json(json.dumps(json_input))
        try:
            json_base_name = f'{json_dict["QUERY_ID"]}.json'
            parquet_base_name = f'{json_dict["QUERY_ID"]}.parquet'
            full_json_file = f'{parquet_dir}/{json_base_name}'
            with open(full_json_file, 'w') as file:
                file.write(json_input)
            parquet_file = os.path.join(parquet_dir, parquet_base_name)
            df.to_parquet(parquet_file, engine='pyarrow', index=False)
            self.log.info(f'Converted JSON input to {parquet_file}')
        except Exception as e:
            self.log.error(e)
        hdfs_path = f'/user/hive/warehouse/{database_name}.db/{table_name}/{parquet_base_name}'
        hdfs_path_tmp = f'/tmp/hive/{database_name}.db/{table_name}/{parquet_base_name}'
        try:
            hdfs_webhdfs_uri = f"{self.hdfs_host}:{self.hdfs_port}"
            client = InsecureClient(f'http://{hdfs_webhdfs_uri}', user='hdfs')
            directory_path = f'/user/hive/warehouse/{database_name}.db/{table_name}'
            directory_path_tmp = f'/tmp/hive/{database_name}.db/{table_name}'
            if not client.status(directory_path, strict=False):
                client.makedirs(directory_path)
                self.log.info(f"Created directory {directory_path} on HDFS")
            if not client.status(directory_path_tmp, strict=False):
                client.makedirs(directory_path_tmp)
                self.log.info(f"Created directory {directory_path_tmp} on HDFS")
            for file in [hdfs_path_tmp, hdfs_path]:
                if client.status(file, strict=False):
                    client.delete(file, recursive=True)
                    self.log.info(f"{file} deleted")
            parquet_file = parquet_file.replace("\\", "/")
            client.upload(hdfs_path_tmp, parquet_file)
            self.log.info(f'Uploaded {parquet_file} to HDFS at {hdfs_path_tmp}')
            client.upload(hdfs_path, parquet_file)
            self.log.info(f'Uploaded {parquet_file} to HDFS at {hdfs_path}')
            print(f'Copied file from {hdfs_path_tmp} to {hdfs_path}')
        except Exception as e:
            self.log.error(f'Failed to upload {parquet_file} to HDFS: {e}')
        conn, cursor = self.CreateTable(database_name, dateField, df, table_name)
        partition_dates = df[dateField].unique()
        for partition_date in partition_dates:
            try:
                partition_date_str = partition_date.strftime('%Y-%m-%d')
                query_txt = f"ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION ({dateField}='{partition_date_str}')"
                cursor.execute(query_txt)
                query_txt = f"ALTER TABLE {table_name}_TMP ADD IF NOT EXISTS PARTITION ({dateField}='{partition_date_str}')"
                cursor.execute(query_txt)
            except Exception as e:
                self.log.error(e)
            try:
                self.log.info(json_dict["eventname"])
                if json_dict["eventname"] == "sql_batch_starting":
                    load_data_query = f"LOAD DATA INPATH '{hdfs_path}' INTO TABLE {table_name}_TMP PARTITION ({dateField}='{partition_date_str}')"
                    cursor.execute(load_data_query)
                    load_data_query = f"INSERT INTO TABLE {table_name} PARTITION (START_TIME) SELECT * FROM {table_name}_TMP"
                    cursor.execute(load_data_query)
                    load_data_query = f"truncate table  {table_name}_TMP"
                    cursor.execute(load_data_query)
                    self.log.info(f"Loaded data from {hdfs_path} into Hive table {table_name}_TMP partition ({dateField}='{partition_date_str}')")
                else:
                    load_data_query = f'UPDATE {table_name} SET END_TIME="{json_dict["event_time_"]}" WHERE object_id="{json_dict["object_id"]}"'
                    cursor.execute(load_data_query)
                    self.log.info(f"Update end_date")
            except Exception as e:
                self.log.error(e)
        cursor.close()
        conn.close()

    def CreateTable(self, database_name, dateField, df, table_name):
        columns = df.columns
        dtypes = df.dtypes
        dtype_mapping = {
            'int64': 'BIGINT',
            'float64': 'DOUBLE',
            'object': 'STRING',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
            'datetime64[ns, UTC]': 'TIMESTAMP'
        }
        hive_columns = []
        for col, dtype in zip(columns, dtypes):
            if col != dateField:
                hive_dtype = dtype_mapping.get(str(dtype), 'STRING')
                hive_columns.append(f"`{col}` {hive_dtype}")
        create_table_columns = ',\n'.join(hive_columns)
        conn = hive.Connection(host=self.hive_server, port=self.hive_port, username='hive')
        cursor = conn.cursor()
        try:
            cursor.execute(f"USE {database_name}")
        except Exception as e:
            self.log.error(e)
        self.CreateTableParquet(create_table_columns, cursor, database_name, dateField, f'{table_name}_TMP')
        self.CreateTableOrc(create_table_columns, cursor, database_name, dateField, table_name)
        return conn, cursor

    def CreateTableParquet(self, create_table_columns, cursor, database_name, dateField, table_name):
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {create_table_columns}
        )
        PARTITIONED BY ({dateField} DATE)
        STORED AS PARQUET       
        """
        try:
            cursor.execute(create_table_query)
            self.log.info(f"Table {table_name} is ready in database {database_name}")
        except Exception as e:
            self.log.error(e)

    def CreateTableOrc(self, create_table_columns, cursor, database_name, dateField, table_name):
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {create_table_columns}
        )
        PARTITIONED BY ({dateField} DATE)
        CLUSTERED BY (QUERY_ID) INTO 10 BUCKETS
        STORED AS ORC
        TBLPROPERTIES ('transactional'='true')
        """
        try:
            cursor.execute(create_table_query)
            self.log.info(f"Table {table_name} is ready in database {database_name}")
        except Exception as e:
            self.log.error(e)

    def save(self, arango_url, database_name, collection_name, query, keyName):
        self.create_collection(arango_url, database_name, collection_name)
        try:
            url = f'{arango_url}/_db/{database_name}/_api/cursor'
            headers = {'Content-Type': 'application/json'}
            payload = json.dumps(query)
            response = requests.post(url, data=payload, headers=headers)
            results = response.json()['result'] if response.status_code == 201 else []
            for document in results:
                upsert_query = {
                    "query": f'''
                        UPSERT @filter INSERT @doc UPDATE @update IN {collection_name}
                    ''',
                    "bindVars": {
                        "filter": {keyName: document[keyName]},
                        "doc": document,
                        "update": document
                    }
                }
                upsert_response = requests.post(url, data=json.dumps(upsert_query))
                if upsert_response.status_code != 201:
                    self.log.error(upsert_response.status_code)
        except Exception as e:
            self.log.error(e)


