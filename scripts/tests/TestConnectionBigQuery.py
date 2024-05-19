import sys
sys.path.append('./')
import unittest
from unittest.mock import MagicMock
from scripts.connection.ConnectionBigQuery import ConnectionBigQuery

class TestConnectionBigQuery(unittest.TestCase):

    def test_get_connection(self):
        # Create a mock client object
        mock_client = MagicMock()

        # Patch the bigquery.Client class to return the mock client object
        with unittest.mock.patch('google.cloud.bigquery.Client', return_value=mock_client):
            # Instantiate ConnectionBigQuery
            conn = ConnectionBigQuery()

            # Call the getConnection method
            client = conn.getConnection()

            # Assert that the returned client is the mock client
            self.assertEqual(client, mock_client)

    def test_querying_success(self):
        # Create a mock client object
        mock_client = MagicMock()
        mock_query_job = MagicMock()
        mock_query_job.to_dataframe.return_value = 'mock_data'
        mock_client.query_and_wait.return_value = mock_query_job

        # Patch the bigquery.Client class to return the mock client object
        with unittest.mock.patch('google.cloud.bigquery.Client', return_value=mock_client):
            # Instantiate ConnectionBigQuery
            conn = ConnectionBigQuery()

            # Call the querying method
            result = conn.querying('SELECT * FROM my_table', 'my_job_config')

            # Assert that the result is the expected mock data
            self.assertEqual(result, 'mock_data')

    def test_querying_failure(self):
        # Create a mock client object
        mock_client = MagicMock()
        mock_client.query_and_wait.side_effect = Exception("Mocked exception")

        # Patch the bigquery.Client class to return the mock client object
        with unittest.mock.patch('google.cloud.bigquery.Client', return_value=mock_client):
            # Instantiate ConnectionBigQuery
            conn = ConnectionBigQuery()

            # Call the querying method
            result = conn.querying('SELECT * FROM my_table', 'my_job_config')

            # Assert that the method prints an error message
            self.assertIsNone(result)  # Since the method returns None on exception, we assert None

if __name__ == '__main__':
    unittest.main()
