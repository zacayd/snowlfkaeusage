import concurrent
import asyncio
import hashlib
import logging
import os
import sys
import time
from time import strftime, gmtime
import xml.etree.ElementTree as ET
import pyarrow.fs as fs
###
from pyhive import hive
from sqlalchemy import create_engine, text
import requests
import json
from datetime import datetime
from confluent_kafka import Consumer, KafkaException

# from DatabricksAnalysis import DatabricksAnalysis
from Logger import Logger
from flask import Flask, jsonify, request

import pandas as pd
import os
from sqlalchemy import create_engine
import pyarrow.parquet as pq
from pyhive import hive
from hdfs import InsecureClient
from subprocess import run
import pyarrow as pa
class ConsumerUsage:

    def generate_md5_hash(self,proc_catalog, proc_schema, proc_name, args):
        # Concatenate the values into a single string
        combined_string = f"{proc_catalog}:{proc_schema}:{proc_name}:{args}"

        # Generate the MD5 hash
        md5_hash = hashlib.md5(combined_string.encode()).hexdigest()

        return md5_hash
    def __init__(self,
                 groupid,
                 brokers,
                 sqlConnectionString,
                 arangoDBUrl,
                 listOfTables,
                 dbvVendor,
                 gspUrl,
                 hiveServer,
                 hivePort,
                 hdfs_host,
                 hdfs_port

                 ):

            self.brokers = brokers
            self.group_id = groupid
            self.sqlConnectionString = sqlConnectionString
            self.arangoDBUrl=arangoDBUrl,
            self.listOfTables=listOfTables,
            self.hive_server=hiveServer,
            self.hive_port=hivePort
            self.log = Logger('Consumer')
            self.hdfs_host=hdfs_host,
            self.hdfs_port=hdfs_port
            self.conf= {
            'bootstrap.servers': brokers,
            'group.id': "123",  # Consumer group ID
            'auto.offset.reset': 'earliest',  # Start from the earliest messages latest
            'enable.auto.commit': False , # Enable automatic offset commit
            'max.poll.interval.ms':600000
            }
            self.consumer=Consumer(self.conf)
            self.dbvVendor=dbvVendor
            self.gspUrl=gspUrl





    def setLogLocation(self, CustomerDatabase):
        CompName = CustomerDatabase
        current_time = datetime.now().strftime("%Y-%m-%d")
        logs_folder = CompName  # Replace with the desired folder path
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
        OumconnectionString = envConnString
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
            result = self.cnxn.execute(text(config['QueryAllCustomers']))
            customers = result.fetchall()
            # serial
            for customer in customers:
                try:
                    self.ProcessCustomer(customer)
                except Exception as e:
                        self.log.error(e)

            # Parallel
            # with concurrent.futures.ThreadPoolExecutor(max_workers=len(customers)) as executor:
            #     # Submit each topic's consumer loop to the executor
            #     futures = [executor.submit(self.ProcessCustomer, customer) for customer in customers]
            #
            #     # Wait for all futures to complete
            #     concurrent.futures.wait(futures)
        except Exception as e:
            self.log.error(e)
        # self.cnxn.close()
        # asyncio.run(self.Process_Customers_Async(customers))

    def ProcessCustomer(self, customer):
        try:
            CompName=customer[1]
            arangoDBUrl = self.arangoDBUrl[0]
            CustomerConnectionString = customer[2]
            CompanyId = customer[0]
            list_of_tables=self.listOfTables[0]

            CustomerServer = CustomerConnectionString.split(';')[0].split('=')[1]
            CustomerDatabase = CustomerConnectionString.split(';')[1].split('=')[1]
            CustomerUsr = CustomerConnectionString.split(';')[2].split('=')[1]
            CustomerPwd = CustomerConnectionString.split(';')[3].split('=')[1]
            CustomerPwd = CustomerPwd.replace('@', '%40')
            customerEngine = create_engine(
                f"mssql+pyodbc://{CustomerUsr}:{CustomerPwd}@{CustomerServer}:{self.port}/{CustomerDatabase}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes")
            customerCnx = customerEngine.connect()
            connresult = customerCnx.execute(text(config['ToolQuery']))
            connections = connresult.fetchall()

            self.setLogLocation(CustomerDatabase)
        except Exception as e:
            self.log.error(e)

        # Serial
        if len(connections)>0:
            for connection in connections:
                self.ProcessTopicMessage(CompName, CompanyId, connection,CustomerDatabase,arangoDBUrl)

        try:
            customerCnx.close()
        except Exception as e:
            self.log.error(e)

    def constructDoc(self,message,url,db_name):

        try:

            result_users=self.GetMessageFromCollection(db_name, message, url,"users",message["user_id"])
            if result_users!=None:
                result_views = self.GetMessageFromCollection(db_name, result_users, url, "views", message["view_id"])
            if result_views!=None:
                result_workbooks = self.GetMessageFromCollection(db_name, result_views, url, "system_users",result_views["users"][0]["system_user_id"])
            if result_workbooks!=None:
               result_projects = self.GetMessageFromCollection(db_name, result_workbooks, url, "workbooks", result_workbooks["views"][0]["workbook_id"])
            if result_projects!=None:
                resultFinal = self.GetMessageFromCollection(db_name, result_projects, url, "projects", result_projects["workbooks"][0]["project_id"])
        except Exception as e:
            self.log.error(e)
        return resultFinal

    def constructDocViews(self,message,url,db_name):

        try:

            result_users=self.GetMessageFromCollection(db_name, message, url,"system_users",message["owner_id"])
            return  result_users

        except Exception as e:
            self.log.error(e)
        return result_users


    def GetMessageFromCollection(self, db_name, message, url,collection_name,id):

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
                message[collection_name]=keys
                return message
            else:
                return  {}
        except Exception as e:
            self.log.error(e)


    def ProcessTopicMessage(self, CompName, CompanyId, connection,CustomerDatabase,arangoDBUrl):
        try:
            arangoDBUrl=f'http://{arangoDBUrl}'
            connectionID = connection[3]
            ToolName = connection[0]
            self.create_database(arangoDBUrl, CustomerDatabase)

            list_of_tables=self.listOfTables[0]


            self.run(arangoDBUrl, CustomerDatabase, CompanyId, CompName, connectionID, ToolName,list_of_tables)
        except Exception as e:
            self.log.error(e)

    def process_topic(self, topic_name,arangoDBUrl,CustomerDatabase,collectionName):
        self.consumer.subscribe([topic_name])
        isDone=True
        start_time = time.time()  # Record the start time
        while not self.done_with_topic(start_time,collectionName) or isDone:
            # done_condition = consumed_records_count >= max_records_to_consume
            # if not done_condition:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                self.log.info(f"No messages from kafka topic:{topic_name}....")
                isDone=False

                continue
            if msg.error():
                # if msg.error().code() == KafkaException._PARTITION_EOF:
                self.log.error(msg.error())

                continue
            else:

                topic_name = msg.topic()
                listOfMessages = json.loads(msg.value().decode('utf-8'))
                self.log.info(f"Topic: {topic_name}, Message: {listOfMessages}")
                collectionName = topic_name.split('_TOPIC_')[-1]

                if collectionName == "http_requests":

                    try:
                        if not(listOfMessages["action"]=='bootstrapSession' and listOfMessages['currentsheet']!=''):
                            listOfMessages=[]
                        else:
                            originalPart=listOfMessages['currentsheet']
                            split_parts = originalPart.split('/')

                            # Insert 'sheets' one before the last
                            split_parts.insert(-1, "sheets")

                            # Join the parts back together with '/'
                            new_string = '/'.join(split_parts)
                            listOfMessages['currentsheet']=new_string


                    except Exception as e:
                        self.log.error(e)
                if  collectionName == "historical_events":
                    if listOfMessages["hist_view_id"]==None:
                        listOfMessages = []


                if len(listOfMessages)>0:
                    if collectionName=="QUERY_HISTORY":
                        self.insert_document(arangoDBUrl, CustomerDatabase, collectionName, listOfMessages,
                                             str(listOfMessages["QUERY_ID"]))

                    elif  collectionName in ["VIEWS","TABLES"]:
                        self.insert_document(arangoDBUrl, CustomerDatabase, collectionName, listOfMessages,
                                             str(listOfMessages["TABLE_ID"]))

                    elif  collectionName in ["COLUMNS"  ]:
                        self.insert_document(arangoDBUrl, CustomerDatabase, collectionName, listOfMessages,
                                             str(listOfMessages["COLUMN_ID"]))

                    elif  collectionName in ["PROCEDURES"]:
                        procCatalog=listOfMessages["PROCEDURE_CATALOG"]
                        procSchema = listOfMessages["PROCEDURE_SCHEMA"]
                        procName = listOfMessages["PROCEDURE_NAME"]
                        args = listOfMessages["ARGUMENT_SIGNATURE"]
                        id=self.generate_md5_hash(procCatalog,procSchema,procName,args)

                        self.insert_document(arangoDBUrl, CustomerDatabase, collectionName, listOfMessages,id)
                    elif collectionName == "http_requests":
                        AQL = config["AQL"]
                        if len(listOfMessages)>0:
                            currentsheet = listOfMessages["currentsheet"]

                            # AQL query
                            query = {
                                "query": AQL,
                                "bindVars": {"currentsheet": currentsheet}
                            }
                            self.save(arangoDBUrl, CustomerDatabase, 'tableau_usage', query,'repositoryUrl')

                    elif collectionName == "historical_events":

                        AQL = config["AQL_AUDIT"]
                        if len(listOfMessages) > 0:
                            id=listOfMessages["id"]
                            # AQL query
                            query = {
                                "query": AQL,
                                "bindVars": {"id": id}
                            }
                            self.save(arangoDBUrl, CustomerDatabase, 'tableau_audit', query,"id")


            self.consumer.commit(msg)

        self.consumer.unsubscribe()

    def done_with_topic(self, start_time,collectionName):
        with open('Config.json', 'r') as f:
            config = json.load(f)
        duration=config["timeToWait"] [collectionName]
        # Check if 5 minutes have passed since start_time
        current_time = time.time()
        if current_time - start_time > duration:  # 120 seconds = 2 minutes
            return True
        return False

    def run(self,arangoDBUrl,CustomerDatabase,CompanyId,CompName,connectionID,ToolName,list_of_tables):
        while True:
            try:
                topics = [f'{CompanyId}_{CompName}_{connectionID}_{ToolName}_TOPIC_{tableName}' for tableName in
                          list_of_tables]
                for topic in topics:
                    collectionName = topic.split('_TOPIC_')[-1].upper()
                    topicv=topic.upper()
                    self.process_topic(topicv,arangoDBUrl,CustomerDatabase,collectionName )

            except Exception as ex:
                # Handle exceptions
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

            # AQL query
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

    def create_collection(self,arango_url,database_name,collection_name):
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

    def create_hive(self,hive_server,hive_port,database_name):
        try:

            conn = hive.Connection(host=hive_server[0], port=hive_port, username='hive')
            cursor = conn.cursor()

            # Create the database if it doesn't exist
            create_db_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
            cursor.execute(create_db_query)
            self.log.info(f"Database {database_name} is ready")
        except Exception as e:
            self.log.error(e)




    def checkIfexists(self,url,db_name,collection_name,keyCheck):

        # Define the AQL query

        aql_query = f"FOR doc IN {collection_name} FILTER doc.id == '{keyCheck}' RETURN doc"


        # Define the headers
        headers = {'Content-Type': 'application/json'}

        # Build the request URL
        request_url = f'{url}/_db/{db_name}/_api/cursor'

        # Create the request payload
        payload = {
            'query': aql_query,
            'count': True
        }

        # Send the POST request to execute the query
        response = requests.post(request_url, headers=headers, data=json.dumps(payload))

        # Check if the request was successful
        if response.status_code == 201:
            # Retrieve the result
            result = response.json()
            keys = result['result']
            if keys:
                return True
            else:
                return False
        else:
            # Handle the error
            self.log.error(f"An error occurred: {response.text}")
            return False

    def put_document(self,arango_url,database_name,collection_name,json_document,keyToCheck):

        try:
            isExists=self.checkIfexists(arango_url,database_name,collection_name,keyToCheck)

            if not isExists:
                url = f'{arango_url}/_db/{database_name}/_api/document/{collection_name}'

                headers = {
                    'Content-Type': 'application/json'
                }
                data=json.loads(json_document)

                if data['QUERY_TEXT']!="<redacted>":
                    txt=data['QUERY_TEXT']
                    # txt="select a.column1,b.col2  from db1.schema1.tab1 a inner join db2.schema2.tabl2 b on a.z = b.z"
                    xml_data=self.GspParsing(txt)

                    root = ET.fromstring(xml_data)
                    xml_dict = self.parse_element(root)
                    # Convert dictionary to JSON
                    json_data = json.dumps(xml_dict, indent=4)
                    json_dict=json.loads(json_data)
                    if json_dict:
                        if 'error' not in json_dict:
                            if "table" in json_dict:
                                if isinstance(json_dict["table"], list):
                                    for table in json_dict["table"]:
                                            for col in table["column"]:
                                                data["db"]=table["database"] if table["database"] else data["DATABASE_NAME"]
                                                data["schemaName"]= table["schema"] if table["schema"] else data["SCHEMA_NAME"]
                                                data["tablename"] = table["name"].split(".")[-1],
                                                data["columnName"]=col["name"]

                                                timestamp_ms = data["START_TIME"]
                                                data["START_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                                data["START_TIME_"] = self.ConvertLinuxToDateTime(timestamp_ms)

                                                timestamp_ms = data["END_TIME"]
                                                data["END_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)

                                                payload = json.dumps(data)
                                                response = requests.post(url, headers=headers, data=payload)
                                                self.json_to_parquet_to_hive(payload, collection_name, database_name,"START_TIME",is_file=True)

                                                response = requests.post(url, headers=headers, data=payload)

                                                if response.status_code in [201, 202]:
                                                    self.log.info("Document put successfully.")
                                                else:
                                                    self.log.error("Error putting document.")
                                else:
                                    if isinstance(json_dict["table"]["column"], list):
                                        for col in json_dict["table"]["column"]:
                                            data["db"] = json_dict["table"]["database"] if json_dict["table"]["database"] else data[
                                                        "DATABASE_NAME"]
                                            data["schemaName"] = json_dict["table"]["schema"] if json_dict["table"]["schema"] else data["SCHEMA_NAME"]
                                            data["tablename"] = json_dict["table"]["name"].split(".")[-1]
                                            data["columnName"] = col["name"]


                                            timestamp_ms=data["START_TIME"]
                                            data["START_TIME"]=self.ConvertLinuxToDateTime(timestamp_ms)

                                            timestamp_ms = data["END_TIME"]
                                            data["END_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                            payload = json.dumps(data)

                                            response = requests.post(url, headers=headers, data=payload)
                                            self.json_to_parquet_to_hive(payload, collection_name, database_name,"START_TIME",is_file=True)


                                            if response.status_code in [201, 202]:
                                                 self.log.info("Document put successfully.")
                                            else:
                                                self.log.error("Error putting document.")
                                    else:
                                        data["db"] = json_dict["table"]["database"] if json_dict["table"][
                                            "database"] else data[
                                            "DATABASE_NAME"]
                                        data["schemaName"] = json_dict["table"]["schema"] if json_dict["table"][
                                            "schema"] else data["SCHEMA_NAME"]
                                        data["tablename"] = json_dict["table"]["name"].split(".")[-1]
                                        data["columnName"] = json_dict["table"]["column"]["name"]


                                        timestamp_ms = data["START_TIME"]
                                        data["START_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)

                                        timestamp_ms = data["END_TIME"]
                                        data["END_TIME"] = self.ConvertLinuxToDateTime(timestamp_ms)
                                        payload = json.dumps(data)

                                        response = requests.post(url, headers=headers, data=payload)
                                        self.json_to_parquet_to_hive(payload, collection_name,database_name,"START_TIME",is_file=True)

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

            else:
                self.log.info("Document already exists")

        except Exception as e:
            self.log.error(e)

    def ConvertLinuxToDateTime(self, timestamp_ms):
        try:
            timestamp_s = timestamp_ms / 1000.0
            # Convert to datetime object
            date_time = datetime.fromtimestamp(timestamp_s)
            date_string = date_time.strftime('%Y-%m-%d %H:%M:%S')

        except Exception as e:
            self.log.error(e)
        return date_string

    def update_document(self, arango_url, database_name, collection_name, json_document, keyToCheck):

        try:
            isExists = self.checkIfexists(arango_url, database_name, collection_name, keyToCheck)

            if not isExists:
                url = f'{arango_url}/_db/{database_name}/_api/document/{collection_name}'

                headers = {
                    'Content-Type': 'application/json'
                }
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

    def parse_element(self,element):
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
    def create_database(self,arango_url,database_name):
        url = f'{arango_url}/_api/database'


        data = {
            'name': database_name
        }

        response = requests.post(url, json=data)

        if response.status_code == 201:
            self.log.info(f"The database '{database_name}' was created.")
        else:
            self.log.info(f"The database {database_name} exits!")




    def GspParsing(self,query):

        payload = json.dumps({
            "query": query,
            "vendor": self.dbvVendor
        })
        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", self.gspUrl, headers=headers, data=payload)
        return response.text

    def insert_document(self,arango_url,database_name,collection_name,json_document,keyToCheck):

        self.create_collection(arango_url,database_name,collection_name)
        self.create_hive(self.hive_server,self.hive_port,database_name)

        jObject=json.dumps(json_document)
        try :
            self.put_document(arango_url,database_name,collection_name,jObject,keyToCheck)

        except Exception as e:
            self.log.error(e)

    def json_to_parquet_to_hive(self, json_input, table_name, database_name, dateField, is_file=True):
        parquet_dir = f'{database_name}/{table_name}'

        # Step 1: Convert JSON to Parquet
        if not os.path.exists(parquet_dir):
            os.makedirs(parquet_dir)

        if is_file:
            try:
                json_dict = json.loads(json_input)
                df = pd.DataFrame([json_dict])
                json_base_name = os.path.basename(json_input)
            except Exception as e:
                self.log.error(e)
        else:
            df = pd.read_json(json.dumps(json_input))
            json_base_name = 'input_data.json'

        # Convert dateField to date if it's not already in date format
        try:
            # df[dateField] = pd.to_datetime(df[dateField]).dt.date
            json_base_name = f'{json_dict["QUERY_ID"]}.json'
            parquet_base_name=f'{json_dict["QUERY_ID"]}.parquet'
            full_json_file = f'{parquet_dir}/{json_base_name}'
            with open(full_json_file, 'w') as file:
                file.write(json_input)

            parquet_file = os.path.join(parquet_dir, parquet_base_name)
            df.to_parquet(parquet_file, engine='pyarrow', index=False)
            self.log.info(f'Converted JSON input to {parquet_file}')
        except Exception as e:
            self.log.error(e)

        # Upload Parquet file to HDFS
            # Step 2: Upload Parquet file to HDFS

        # Step 2: Upload Parquet file to HDFS
        hdfs_path = f'/user/hive/warehouse/{database_name}.db/{table_name}/{parquet_base_name}'
        hdfs_path_tmp = f'/tmp/hive/{database_name}.db/{table_name}/{parquet_base_name}'



        try:
            hdfs_webhdfs_uri=f"{self.hdfs_host[0]}:{self.hdfs_port}"

            client = InsecureClient(f'http://{hdfs_webhdfs_uri}', user='hdfs')

            # Create the directory if it doesn't exist
            directory_path = f'/user/hive/warehouse/{database_name}.db/{table_name}'
            directory_path_tmp=f'/tmp/hive/{database_name}.db/{table_name}'

            if not client.status(directory_path, strict=False):
                client.makedirs(directory_path)
                self.log.info(f"Created directory {directory_path} on HDFS")

            if not client.status(directory_path_tmp, strict=False):
                client.makedirs(directory_path_tmp)
                self.log.info(f"Created directory {directory_path_tmp} on HDFS")

            for file in [hdfs_path_tmp,hdfs_path]:
                if client.status(file, strict=False):
                    client.delete(file, recursive=True)
                    self.log.info(f"{file} deleted")

            parquet_file = parquet_file.replace("\\", "/")
            client.upload(hdfs_path_tmp, parquet_file)
            self.log.info(f'Uploaded {parquet_file} to HDFS at {hdfs_path_tmp}')

            client.upload(hdfs_path, parquet_file)
            self.log.info(f'Uploaded {parquet_file} to HDFS at {hdfs_path}')

            #

            print(f'Copied file from {hdfs_path_tmp} to {hdfs_path}')


        except Exception as e:
            self.log.error(f'Failed to upload {parquet_file} to HDFS: {e}')

        # Step 2: Infer schema from DataFrame
        columns = df.columns
        dtypes = df.dtypes

        # Mapping pandas dtypes to Hive dtypes
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
            if col != dateField:  # Exclude partition column from main column list
                hive_dtype = dtype_mapping.get(str(dtype), 'STRING')
                hive_columns.append(f"`{col}` {hive_dtype}")

        create_table_columns = ',\n'.join(hive_columns)

        # Connect to Hive
        conn = hive.Connection(host=self.hive_server[0], port=self.hive_port, username='hive')
        cursor = conn.cursor()

        # Switch to the database
        try:
            cursor.execute(f"USE {database_name}")
        except Exception as e:
            self.log.error(e)

        # Create the table if it doesn't exist with partitioning
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {create_table_columns}
        )
        PARTITIONED BY ({dateField} TIMESTAMP)
        STORED AS PARQUET
        """
        try:
            cursor.execute(create_table_query)
            self.log.info(f"Table {table_name} is ready in database {database_name}")
        except Exception as e:
            self.log.error(e)

        # Get unique partition dates from the DataFrame
        partition_dates = df[dateField].unique()

        # Add partitions and load data for each unique date
        for partition_date in partition_dates:
            partition_datev = datetime.strptime(partition_date, '%Y-%m-%d %H:%M:%S')

            partition_date_str = partition_datev.strftime('%Y-%m-%d')

            # Add partition if not exists
            cursor.execute(f"ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION ({dateField}='{partition_date_str}')")

            # Load data for the specific partition
            try:
                load_data_query = f"""
                LOAD DATA INPATH '{hdfs_path}' INTO TABLE {table_name} PARTITION ({dateField}='{partition_date_str}')
                """
                cursor.execute(load_data_query)
                self.log.info(
                    f"Loaded data from {hdfs_path} into Hive table {table_name} partition ({dateField}='{partition_date_str}')")
            except Exception as e:
                self.log.error(e)


        cursor.close()
        conn.close()


    def save(self,arango_url,database_name,collection_name,query,keyName):

        self.create_collection(arango_url,database_name,collection_name)

        try :
            url = f'{arango_url}/_db/{database_name}/_api/cursor'

            headers = {
                'Content-Type': 'application/json'
            }

            payload=json.dumps(query)
            response = requests.post(url, data=payload,headers=headers)

            results = response.json()['result'] \
                if response.status_code == 201 else []
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

                # Send the upsert request
                upsert_response = requests.post(url,data=json.dumps(upsert_query))

                if upsert_response.status_code != 201:
                    self.log.error(upsert_response.status_code)

        except Exception as e:
            self.log.error(e)





class ConsumerExecution:
    def __init__(self):

        self.brokers = kafka_broker
        self.group_id = 'myGroup'
        self.sqlConnectionString = envConnString
        self.dbvVendor=dbvVendor
        self.gspUrl=gspUrl

        self.hive_server = hive_server
        self.hive_port = hive_port
        self.hdfs_host=hdfs_host
        self.hdfs_port = hdfs_port

        try:
            self.p1 = ConsumerUsage(
                self.group_id,
                self.brokers,
                self.sqlConnectionString,
                arangoDBUrlServer,
                list_of_tables,
                self.dbvVendor,
                self.gspUrl,
                self.hive_server,
                self.hive_port,
                self.hdfs_host,
                self.hdfs_port

            )

        except Exception as e:
            print(e)

    def run(self):
        self.p1.log.info("Start..")
        try:
            self.p1.GetMessagesFromTopic()
        except Exception as e:
            self.p1.log.error(e)


def TestDBConnection(connectionString):
    try:

        server = connectionString.split(';')[0].split('=')[1]
        database = connectionString.split(';')[1].split('=')[1]
        usr = connectionString.split(';')[2].split('=')[1]
        pwd = connectionString.split(';')[3].split('=')[1]
        pwd = pwd.replace('@', '%40')
        port = "1433"
        engine = create_engine(
            f"mssql+pyodbc://{usr}:{pwd}@{server}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes")
        cnxn = engine.connect()
        cnxn.close()
        engine.dispose()
        return jsonify({"status": "success", "message": "success"}), 200
    except Exception as e:
        cnxn.close()
        engine.dispose()
        return jsonify({"status": "failed", "message": e}), 400


def TestArangoDB(arangoDBUrl):
    try:

        url = f"http://{arangoDBUrl}/_api/version"

        response = requests.get(url)

        if response.status_code == 200:
            return jsonify({"status": "success", "message": response.text}), response.status_code
        else:
            return jsonify({"status": "success", "message": response.text}), response.status_code
    except Exception as e:
        return jsonify({"status": "failed", "message": e}), 400


app = Flask(__name__)


@app.route('/runUsageConsumer', methods=['GET'])
def run_api():
    with open('Config.json', 'r') as f:
        global config, server, usr, pwd, engine, cnxn, arangoDBUrlServer, envConnString, kafka_broker, GspUrl,list_of_tables,gspUrl,dbvVendor,hive_server,hive_port,hdfs_host,hdfs_port
        try:
            configJsonDict = UploadConfig(f)
            envConnString = configJsonDict['OumDBConnectionString']
            arangoDBUrlServer = configJsonDict['arangoDBUrl']
            kafka_broker = configJsonDict['kafka_broker']
            list_of_tables=configJsonDict['list_of_tables']
            gspUrl=configJsonDict['GspUrl']
            dbvVendor=configJsonDict['dbvVendor']
            hive_server=configJsonDict['hive_server']
            hive_port = configJsonDict['hive_port']
            hdfs_host=configJsonDict['hdfs_host']
            hdfs_port=configJsonDict['hdfs_port']

            cnxn.close()
            engine.dispose()
        except Exception as e:
            print(e)
            cnxn.close()
            engine.dispose()

    consumerEx = ConsumerExecution()
    consumerEx.run()
    return jsonify({"status": "success", "message": "Run completed"}), 200


@app.route('/HealthUsageConsumer', methods=['GET'])
def Health():
    with open('Config.json', 'r') as f:
        global config, server, usr, pwd, engine, cnxn, kafka_broker, envConnString, arangoDBUrlServer, GspUrl
        try:
            config = json.load(f)
            connectionString = config['OumDBConnectionString']
            server = connectionString.split(';')[0].split('=')[1]
            database = connectionString.split(';')[1].split('=')[1]
            usr = connectionString.split(';')[2].split('=')[1]
            pwd = connectionString.split(';')[3].split('=')[1]
            pwd = pwd.replace('@', '%40')
            port = "1433"
            engine = create_engine(
                f"mssql+pyodbc://{usr}:{pwd}@{server}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes")
            cnxn = engine.connect()
            query = text(config['EnvConfigQuery'])
            result = cnxn.execute(query)
            dataconfig = result.fetchall()
            configJson = dataconfig[0].TOOL_CONFIG_JSON
            configJsonDict = json.loads(configJson)
            envConnString=connectionString
            arangoDBUrlServer = configJsonDict['arangoDBUrl']
            kafka_broker = configJsonDict['kafka_broker']
            GspUrl = configJsonDict['GspUrl']

            cnxn.close()
            engine.dispose()
            resultSql = TestDBConnection(envConnString)
            resultArangoDB = TestArangoDB(arangoDBUrlServer)

            sqlMessage = json.loads(resultSql[0].response[0])['message']
            arangoDBMessage = json.loads(resultArangoDB[0].response[0])['message']

            if resultSql[1] == 200 and resultArangoDB[1] == 200:
                return jsonify({"status": "success", "message": "success"}), 200
            else:
                return jsonify(
                    {"status": "failed", "message": f'{sqlMessage} {arangoDBMessage}'}), 500
        except Exception as e:
            print(e)
            cnxn.close()
            engine.dispose()
            return jsonify({"status": "failed", "message": "Failed to connect to Database"}), 400
    # agentEx = AgentExecution()
    # agentEx.runAllCustomersTopics()
    #
    # return jsonify({"status": "success", "message": "Run completed"}), 200


def UploadConfig(f):

    try:
        global config, server, usr, pwd, engine, cnxn
        config = json.load(f)
        connectionString = config['OumDBConnectionString']
        server = connectionString.split(';')[0].split('=')[1]
        database = connectionString.split(';')[1].split('=')[1]
        usr = connectionString.split(';')[2].split('=')[1]
        pwd = connectionString.split(';')[3].split('=')[1]
        pwd = pwd.replace('@', '%40')
        port = "1433"
        sqlConnString=f"mssql+pyodbc://{usr}:{pwd}@{server}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
        engine = create_engine(sqlConnString)

        cnxn = engine.connect()
        result = cnxn.execute(text(config['EnvConfigQuery']))
        dataconfig = result.fetchall()
        configJson = dataconfig[0][0]
        configJsonDict = json.loads(configJson)
        cnxn.close()
        engine.dispose()



    except Exception as e:
        print(e)

    return configJsonDict


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=7002)
