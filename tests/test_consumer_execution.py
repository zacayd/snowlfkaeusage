import unittest
from unittest.mock import patch
from consumers.consumer_execution import ConsumerExecution

class TestConsumerExecution(unittest.TestCase):
    @patch('consumers.consumer_execution.ConsumerUsage')
    @patch('utils.logger.Logger')
    def test_run(self, mock_logger, mock_consumer_usage):
        mock_config = {
            'OumDBConnectionString': 'test_connection_string',
            'QueryAllCustomers': 'SELECT * FROM customers'
        }
        consumer_execution = ConsumerExecution(mock_config)
        consumer_execution.run()
        self.assertTrue(mock_consumer_usage().get_messages_from_topic.called)

if __name__ == '__main__':
    unittest.main()
