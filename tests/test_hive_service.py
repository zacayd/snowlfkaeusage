import unittest
from unittest.mock import patch, MagicMock
from services.hive_service import HiveService

class TestHiveService(unittest.TestCase):
    @patch('services.hive_service.hive.Connection')
    @patch('utils.logger.Logger')
    def test_create_database(self, mock_logger, mock_hive_connection):
        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_hive_connection.return_value = mock_connection

        hive_service = HiveService({'hive_server': 'test_server', 'hive_port': 10000})
        hive_service.create_database('test_db')

        mock_cursor.execute.assert_called_with('CREATE DATABASE IF NOT EXISTS test_db')

    # Add more tests as needed

if __name__ == '__main__':
    unittest.main()
