import sys
sys.path.append('./')

import unittest
import pandas as pd
from unittest.mock import patch, MagicMock
from scripts.experimentation.ProcessLocationData import ProcessLocationData # Ensure this import matches your module's structure

class TestProcessLocationData(unittest.TestCase):

    def setUp(self):
        # Initialize the DataProcessor object
        self.processor = ProcessLocationData()

    @patch('pandas.read_csv')
    def test_process_departments(self, mock_read_csv):
        # Mock the pandas read_csv method
        mock_read_csv.return_value = pd.DataFrame({
            "Latitude la plus au nord": [50.0],
            "Latitude la plus au sud": [40.0],
            "Longitude la plus à l’ouest": [-5.0],
            "Longitude la plus à l’est": [10.0]
        })

        departments = self.processor.process_departments()

        # Verify the columns were renamed correctly
        self.assertIn("Nothernmost_latitude", departments.columns)
        self.assertIn("Southernmost_latitude", departments.columns)
        self.assertIn("Furthestwest_longitude", departments.columns)
        self.assertIn("Furthesteast_longitude", departments.columns)

    @patch('pandas.read_csv')
    def test_get_list_localisation(self, mock_read_csv):
        mock_read_csv.return_value = pd.DataFrame({
            'mp_latitude': [48.8566, 45.7640],
            'mp_longitude': [2.3522, 4.8357]
        })

        localisations = self.processor.get_list_localisation()

        expected_localisations = [(48.8566, 2.3522), (45.7640, 4.8357)]
        self.assertEqual(localisations, expected_localisations)

    @patch('pandas.read_csv')
    @patch.object(ProcessLocationData, 'process_departments')
    def test_get_list_departements(self, mock_process_departments, mock_read_csv):
        mock_process_departments.return_value = pd.DataFrame({
            "Nothernmost_latitude": [50.0],
            "Southernmost_latitude": [40.0],
            "Furthesteast_longitude": [10.0],
            "Furthestwest_longitude": [-5.0],
            "Departement": ["Department1"]
        })

        departments = self.processor.get_list_departements()

        expected_departments = [(50.0, 40.0, 10.0, -5.0, "Department1")]
        self.assertEqual(departments, expected_departments)

    def test_checking_date(self):
        self.assertTrue(self.processor.checking_date("2020-01-01 00:00:00", "2021-12-27 00:00:00"))
        self.assertFalse(self.processor.checking_date("2021-12-27 00:00:00", "2020-01-01 00:00:00"))
        self.assertIsNone(self.processor.checking_date("invalid-date", "2020-01-01 00:00:00"))

    @patch('pandas.read_csv')
    def test_get_partnerships(self, mock_read_csv):
        mock_read_csv.return_value = pd.DataFrame({
            'partnership_type': ['SAS', 'SARL']
        })
        mock_os_path_exists = patch('scripts.experimentation.ProcessLocationData.os.path.exists', return_value=True).start()

        partnerships = self.processor.get_partnerships()

        self.assertEqual(partnerships, ['SAS', 'SARL'])

        mock_os_path_exists.stop()

    # Add more tests for other methods as needed

if __name__ == '__main__':
    unittest.main()
