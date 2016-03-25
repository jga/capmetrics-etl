import os
import unittest
from capmetrics_etl import etl


class TestETL(unittest.TestCase):

    def setUp(self):
        tests_path = os.path.dirname(__file__)
        self.test_excel = os.path.join(tests_path, 'data/test_cmta_data.xls')

    def test_get_season_and_year(self):
        periods = ['Winter    2014', 'Spring 2015', 'Summer  2013', 'Fall 1999']
        self.assertEqual(('winter', '2014'), etl.get_season_and_year(periods[0]))
        self.assertEqual(('spring', '2015'), etl.get_season_and_year(periods[1]))
        self.assertEqual(('summer', '2013'), etl.get_season_and_year(periods[2]))
        self.assertEqual(('fall', '1999'), etl.get_season_and_year(periods[3]))

    def test_get_bad_season_and_year(self):
        periods = ['Winter    14', 'June 2015', 'Summer-2013', 'Fall 99']
        for period in periods:
            self.assertEqual((None, None), etl.get_season_and_year(period))

    def test_get_route_info(self):
        result = etl.get_route_info(self.test_excel, 'Ridership by Route Weekday')
        self.assertTrue(result['numbers_available'])
        self.assertTrue(result['names_available'])
        self.assertEqual(len(result['route_numbers']), 88, msg=result['route_numbers'])
        self.assertEqual(len(result['route_names']), 88, msg=result['route_names'])
