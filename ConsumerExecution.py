from ConsumerUsage import ConsumerUsage

class ConsumerExecution:

    def __init__(self, data, config):
        self.brokers = data['kafka_broker']
        self.group_id = 'myGroup'
        self.sqlConnectionString = data['OumDBConnectionString']
        self.dbvVendor = data['dbvVendor']
        self.gspUrl = data['GspUrl']
        self.hive_server = data['hive_server']
        self.hive_port = data['hive_port']
        self.hdfs_host = data['hdfs_host']
        self.hdfs_port = data['hdfs_port']
        self.arangoDBUrl = data['arangoDBUrl']
        self.list_of_tables = data['list_of_tables']
        self.data=config


        try:
            self.p1 = ConsumerUsage(
                self.group_id, self.brokers, self.sqlConnectionString, self.arangoDBUrl,
                self.list_of_tables, self.dbvVendor, self.gspUrl, self.hive_server, self.hive_port,
                self.hdfs_host, self.hdfs_port,self.data
            )
        except Exception as e:
            print(e)

    def run(self):
        self.p1.log.info("Start..")
        try:
            self.p1.GetMessagesFromTopic()
        except Exception as e:
            self.p1.log.error(e)
