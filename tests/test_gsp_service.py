import unittest
from unittest.mock import patch, MagicMock
from services.gsp_service import GspService

class TestGspService(unittest.TestCase):
    @patch('services.gsp_service.requests.post')
    def test_gsp_parsing(self, mock_post):
        mock_response = MagicMock()
        mock_response.text = 'parsed_result'
        mock_post.return_value = mock_response

        gsp_service = GspService('test_vendor', 'test_url')
        result = gsp_service.gsp_parsing('test_query')
        self.assertEqual(result, 'parsed_result')

    # Add more tests as needed

if __name__ == '__main__':
    unittest.main()
