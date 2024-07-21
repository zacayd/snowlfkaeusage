from hdfs import InsecureClient

class HdfsService:
    def __init__(self, config, logger):
        self.hdfs_host = config['hdfs_host']
        self.hdfs_port = config['hdfs_port']
        self.logger = logger

    def upload_file(self, local_file_path, hdfs_file_path):
        try:
            hdfs_webhdfs_uri = f"{self.hdfs_host}:{self.hdfs_port}"
            client = InsecureClient(f'http://{hdfs_webhdfs_uri}', user='hdfs')

            directory_path = '/'.join(hdfs_file_path.split('/')[:-1])
            if not client.status(directory_path, strict=False):
                client.makedirs(directory_path)
                self.logger.info(f"Created directory {directory_path} on HDFS")

            client.upload(hdfs_file_path, local_file_path)
            self.logger.info(f'Uploaded {local_file_path} to HDFS at {hdfs_file_path}')
        except Exception as e:
            self.logger.error(f'Failed to upload {local_file_path} to HDFS: {e}')
