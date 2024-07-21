import unittest
from unittest.mock import patch
from utils.config_loader import load_config

class TestConfigLoader(unittest.TestCase):
    @patch('builtins.open')
    @patch('json.load')
    def test_load_config(self, mock_json_load, mock_open):
        mock_json_load.return_value = {'key': 'value'}
        config = load_config()
        self.assertEqual(config, {'key': 'value'})

    # Add more tests as needed

if __name__ == '__main__':
    unittest.main()
