import unittest
from unittest.mock import patch, MagicMock
from database.arango_db import ArangoDB

class TestArangoDB(unittest.TestCase):
    @patch('database.arango_db.requests.get')
    def test_arango_db_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = 'success'
        mock_get.return_value = mock_response

        arango_db = ArangoDB('test_arango_db_url')
        response, status_code = arango_db.test_connection()
        self.assertEqual(status_code, 200)

    @patch('database.arango_db.requests.get')
    def test_arango_db_failure(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        arango_db = ArangoDB('test_arango_db_url')
        response, status_code = arango_db.test_connection()
        self.assertEqual(status_code, 500)

if __name__ == '__main__':
    unittest.main()
