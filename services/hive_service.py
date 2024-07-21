from pyhive import hive

class HiveService:
    def __init__(self, config):
        self.hive_server = config['hive_server']
        self.hive_port = config['hive_port']
        self.log = Logger('HiveService')

    def create_database(self, database_name):
        try:
            conn = hive.Connection(host=self.hive_server, port=self.hive_port, username='hive')
            cursor = conn.cursor()
            create_db_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
            cursor.execute(create_db_query)
            self.log.info(f"Database {database_name} is ready")
        except Exception as e:
            self.log.error(e)
