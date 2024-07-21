import unittest
from unittest.mock import patch, MagicMock
from services.hdfs_service import HdfsService

class TestHdfsService(unittest.TestCase):
    @patch('services.hdfs_service.InsecureClient')
    @patch('utils.logger.Logger')
    def test_upload_file(self, mock_logger, mock_insecure_client):
        mock_client = MagicMock()
        mock_insecure_client.return_value = mock_client

        hdfs_service = HdfsService({'hdfs_host': 'test_host', 'hdfs_port': 50070}, mock_logger)
        hdfs_service.upload_file('local_file_path', 'hdfs_file_path')

        self.assertTrue(mock_client.upload.called)

    # Add more tests as needed

if __name__ == '__main__':
    unittest.main()
