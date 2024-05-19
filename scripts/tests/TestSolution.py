import sys
sys.path.append('./')
import unittest
from unittest.mock import patch, MagicMock

from scripts.solution.Solution import Solution
import pandas as pd
import os
from datetime import datetime

class TestSolution(unittest.TestCase):
    
    @patch('scripts.solution.Solution.ConnectionBigQuery')
    def test_get_date_by_instructors(self, MockConnectionBigQuery):
        # Arrange
        mock_conn = MockConnectionBigQuery.return_value
        mock_df = pd.DataFrame({
            'instructor_id': [1, 2],
            'lesson_start_at': ['2020-08-01', '2020-08-02']
        })
        mock_conn.querying.return_value = mock_df
        
        solution = Solution()
        
        # Act
        solution.get_date_by_instructors()
        
        # Assert
        mock_conn.querying.assert_called_once()
        self.assertTrue(os.path.exists(solution.result_dir + "result_1.csv"))

    @patch('scripts.solution.Solution.ConnectionBigQuery')
    def test_get_partnerships_existing_file(self, MockConnectionBigQuery):
        
        # Arrange
        mock_df = pd.DataFrame({
            'partnership_type': ['SARL', 'SAS']
        })
        mock_df.to_csv('partnerships.csv', index=False)
        
        solution = Solution()
        
        # Act
        partnerships = solution.get_partnerships()
        
        # Assert
        self.assertEqual(partnerships, ['SARL', 'SAS'])
        
        # Clean up
        os.remove('partnerships.csv')

    @patch('scripts.solution.Solution.ConnectionBigQuery')
    def test_get_partnerships_querying(self, MockConnectionBigQuery):
        
        # Arrange
        mock_conn = MockConnectionBigQuery.return_value
        mock_df = pd.DataFrame({
            'partnership_type': ['EI', 'ME']
        })
        mock_conn.querying.return_value = mock_df
        
        solution = Solution()
        
        # Act
        partnerships = solution.get_partnerships()
        
        # Assert
        mock_conn.querying.assert_called_once()
        self.assertEqual(partnerships, ['EI', 'ME'])
        self.assertTrue(os.path.exists('partnerships.csv'))
        
        # Clean up
        os.remove('partnerships.csv')

    def test_checking_date_valid(self):
        solution = Solution()
        start_date = '2020-01-01 00:00:00'
        end_date = '2020-01-02 00:00:00'
        self.assertTrue(solution.checking_date(start_date, end_date))

    def test_checking_date_invalid(self):
        solution = Solution()
        start_date = '2020-01-02 00:00:00'
        end_date = '2020-01-01 00:00:00'
        self.assertFalse(solution.checking_date(start_date, end_date))

    def test_sub_lists(self):
        solution = Solution()
        list1 = ['SARL', 'SAS', 'ME']
        list2 = ['SAS', 'ME']
        self.assertEqual(solution.sub_lists(list1, list2), ['SAS', 'ME'])

    
    

if __name__ == '__main__':
    unittest.main()
