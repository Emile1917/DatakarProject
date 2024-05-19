import sys
sys.path.append('./')

import unittest
from unittest.mock import patch, MagicMock
from scripts.experimentation.Extraction import Extraction # Adjust the import to match the name of your script file
import pandas as pd

class TestExtraction(unittest.TestCase):

    def setUp(self):
        self.project = "test-data-engineer-090621"
        self.dataset_id = "test_dataset"
        self.tables_path = './data/tables/'
        self.extractor = Extraction(self.project, self.dataset_id, self.tables_path)

    @patch('scripts.connection.ConnectionBigQuery.ConnectionBigQuery')
    def test_get_all_tables_from_db(self, mock_connection):
        # Mock response data
        mock_conn_instance = mock_connection.return_value
        mock_conn_instance.querying.return_value = pd.DataFrame({'table_name': ['bookings', 'meeting_points','instructors','lessons']})

        # Call the method being tested
        tables = self.extractor.get_all_tables_from_db()

        # Assert that the method returned the expected result
        self.assertEqual(tables, ['bookings', 'meeting_points','instructors','lessons'])

    @patch('scripts.connection.ConnectionBigQuery.bigquery.Client')
    @patch('scripts.connection.ConnectionBigQuery.ConnectionBigQuery')
    @patch('pandas.DataFrame.to_csv')
    def test_write_tables_locally(self, mock_to_csv, mock_connection, mock_client):
        # Mock get_all_tables_from_db response
        self.extractor.get_all_tables_from_db = MagicMock(return_value=['instructors'])

        # Mock BigQuery client responses
        mock_conn_instance = mock_connection.return_value
        mock_client_instance = mock_client.return_value
        mock_table = MagicMock()
        mock_client_instance.get_table.return_value = mock_table
        mock_client_instance.list_rows.return_value.to_dataframe.return_value = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})

        # Call the method being tested
        self.extractor.write_tables_locally()

        # Assert that to_csv was called with the correct arguments
        mock_to_csv.assert_called_with(self.tables_path + 'instructors.csv', index=False)

if __name__ == '__main__':
    unittest.main()
