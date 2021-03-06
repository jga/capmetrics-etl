import os
import unittest
from capmetrics_etl import quality


class TestQualityAssurance(unittest.TestCase):

    def setUp(self):
        self.worksheet_names = [
            "Ridership by Route Weekday",
            "Ridership by Route Saturday",
            "Ridership by Route Sunday",
            "Riders per Hour Weekday",
            "Riders Hour Saturday",
            "Riders per Hour Sunday"
        ]
        tests_path = os.path.dirname(__file__)
        self.test_excel = os.path.join(tests_path, 'data/test_cmta_data.xls')

    def test_worksheet_completeness_check(self):
        has_worksheets, missing = quality.check_worksheet_completeness(self.test_excel,
                                                                       self.worksheet_names)
        self.assertTrue(has_worksheets, msg=missing)

    def test_check_route_info(self):
        self.assertTrue(quality.check_route_info(self.test_excel, 'Ridership by Route Weekday'))
        self.assertTrue(quality.check_route_info(self.test_excel, 'Ridership by Route Saturday'))
        self.assertTrue(quality.check_route_info(self.test_excel, 'Ridership by Route Sunday'))

    def test_check_ridership_columns(self):
        self.assertTrue(quality.check_for_ridership_columns(self.test_excel,
                                                            self.worksheet_names))
