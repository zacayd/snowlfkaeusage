import unittest
from unittest.mock import patch, MagicMock
from database.sql_server import SQLServer

class TestSQLServer(unittest.TestCase):
    @patch('database.sql_server.create_engine')
    def test_sql_server_success(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value = mock_connection
        mock_create_engine.return_value = mock_engine

        sql_server = SQLServer('test_connection_string')
        response, status_code = sql_server.test_connection()
        self.assertEqual(status_code, 200)

    @patch('database.sql_server.create_engine')
    def test_sql_server_failure(self, mock_create_engine):
        mock_create_engine.side_effect = Exception("Connection failed")

        sql_server = SQLServer('test_connection_string')
        response, status_code = sql_server.test_connection()
        self.assertEqual(status_code, 400)

if __name__ == '__main__':
    unittest.main()
