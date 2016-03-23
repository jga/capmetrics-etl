import os
import unittest
from capmetrics_etl import quality


class TestQualityAssurance(unittest.TestCase):

    def setUp(self):
        tests_path = os.path.dirname(__file__)
        self.test_excel = os.path.join(tests_path, 'test_data/test_cmta_data.xls')

    def test_worksheet_check(self):
        has_worksheets, missing = quality.check_worksheets(self.test_excel)
        self.assertTrue(has_worksheets, msg=missing)

    def test_get_route_numbers(self):
        result = quality.check_route_info(self.test_excel, 'Ridership by Route Weekday')
        self.assertTrue(result['numbers_available'])
        self.assertTrue(result['names_available'])
        self.assertEqual(len(result['route_numbers']), 88, msg=result['route_numbers'])
        self.assertEqual(len(result['route_names']), 88, msg=result['route_names'])
