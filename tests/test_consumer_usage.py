import unittest
from unittest.mock import patch, MagicMock
from consumers.consumer_usage import ConsumerUsage

class TestConsumerUsage(unittest.TestCase):
    @patch('consumers.consumer_usage.create_engine')
    @patch('consumers.consumer_usage.Logger')
    def test_get_messages_from_topic(self, mock_logger, mock_create_engine):
        mock_config = {
            'OumDBConnectionString': 'test_connection_string',
            'QueryAllCustomers': 'SELECT * FROM customers'
        }
        consumer = ConsumerUsage(mock_config)
        consumer.get_messages_from_topic()
        self.assertTrue(mock_create_engine.called)

    # Add more tests as needed

if __name__ == '__main__':
    unittest.main()
