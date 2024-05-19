import sys
sys.path.append('./')
import unittest
from unittest.mock import patch, MagicMock
from scripts.connection.Req import Req

class TestReq(unittest.TestCase):

    @patch('requests.get')
    def test_get_department_success(self, mock_get):
        # Mock response data
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {
                    "components": {
                        "postcode": "75001"
                    }
                }
            ]
        }
        # Configure the mock to return the mocked response
        mock_get.return_value = mock_response

        # Instantiate Req class
        req = Req()

        # Call the method being tested
        department = req.get_department(48.8566, 2.3522)

        # Assert that the method returned the expected result
        self.assertEqual(department, "75")

    @patch('requests.get')
    def test_get_department_failure(self, mock_get):
        # Mock response data for failed request
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        # Instantiate Req class
        req = Req()

        # Call the method being tested
        department = req.get_department(48.8566, 2.3522)

        # Assert that the method returned an empty string
        self.assertEqual(department, "")

if __name__ == '__main__':
    unittest.main()
