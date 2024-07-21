import unittest
from unittest.mock import patch, MagicMock
from services.kafka_service import KafkaService

class TestKafkaService(unittest.TestCase):
    @patch('services.kafka_service.Consumer')
    @patch('utils.logger.Logger')
    def test_process_topic(self, mock_logger, mock_consumer):
        mock_config = {
            'brokers': 'test_broker',
            'group_id': 'test_group'
        }
        kafka_service = KafkaService(mock_config, mock_logger)
        kafka_service.process_topic('test_topic', 'test_url', 'test_db', 'test_collection')
        self.assertTrue(mock_consumer().subscribe.called)

    # Add more tests as needed

if __name__ == '__main__':
    unittest.main()
