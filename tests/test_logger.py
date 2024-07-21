import unittest
from utils.logger import Logger

class TestLogger(unittest.TestCase):
    def test_logger_info(self):
        logger = Logger('TestLogger')
        with self.assertLogs('TestLogger', level='INFO') as cm:
            logger.info('test_info_message')
        self.assertIn('INFO:TestLogger:test_info_message', cm.output)

    def test_logger_error(self):
        logger = Logger('TestLogger')
        with self.assertLogs('TestLogger', level='ERROR') as cm:
            logger.error('test_error_message')
        self.assertIn('ERROR:TestLogger:test_error_message', cm.output)

    # Add more tests as needed

if __name__ == '__main__':
    unittest.main()
