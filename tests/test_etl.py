import configparser
from datetime import datetime
import os
import unittest
import pytz
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import xlrd
from capmetrics_etl import cli, etl, models

APP_TIMEZONE = pytz.timezone('America/Chicago')


class CheckForHeaderTests(unittest.TestCase):

    def setUp(self):
        tests_path = os.path.dirname(__file__)
        self.test_excel = os.path.join(tests_path, 'data/test_cmta_data.xls')
        excel_book = xlrd.open_workbook(filename=self.test_excel)
        self.worksheet = excel_book.sheet_by_name('Ridership by Route Weekday')

    def test_cell_with_headers(self):
        worksheet_routes = {
            'numbers_available': False,
            'names_available': False,
            'types_available': False,
            'routes': [],
        }
        cell = self.worksheet.cell(4, 0)
        result = etl.check_for_headers(cell, self.worksheet, 4, worksheet_routes)
        self.assertTrue(result)
        self.assertEqual(worksheet_routes['numbers_available'], True)
        self.assertEqual(worksheet_routes['names_available'], True)
        self.assertEqual(worksheet_routes['types_available'], True)

    def test_cell_without_headers(self):
        worksheet_routes = {
            'numbers_available': False,
            'names_available': False,
            'types_available': False,
            'routes': [],
        }
        cell = self.worksheet.cell(50, 0)
        result = etl.check_for_headers(cell, self.worksheet, 50, worksheet_routes)
        self.assertFalse(result)
        self.assertEqual(worksheet_routes['numbers_available'], False)
        self.assertEqual(worksheet_routes['names_available'], False)
        self.assertEqual(worksheet_routes['types_available'], False)


class GetSeasonYearTests(unittest.TestCase):

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


class GetRouteInfoTests(unittest.TestCase):

    def setUp(self):
        tests_path = os.path.dirname(__file__)
        self.test_excel = os.path.join(tests_path, 'data/test_cmta_data.xls')

    def test_get_route_info(self):
        result = etl.get_route_info(self.test_excel, 'Ridership by Route Weekday')
        self.assertTrue(result['numbers_available'])
        self.assertTrue(result['names_available'])
        self.assertEqual(len(result['routes']), 88, msg=result['routes'])
        route_info = result['routes'][0]
        self.assertEqual(route_info['route_number'], '1')
        self.assertEqual(route_info['route_name'], '1-NORTH LAMAR/SOUTH CONGRESS')
        self.assertEqual(route_info['service_type'], 'LOCAL')
        route_info2 = result['routes'][1]
        self.assertEqual(route_info2['route_number'], '2')
        self.assertEqual(route_info2['route_name'], '2-ROSEWOOD')
        self.assertEqual(route_info2['service_type'], 'LOCAL')
        route_info10 = result['routes'][9]
        self.assertEqual(route_info10['route_number'], '18')
        self.assertEqual(route_info10['route_name'], '18-MARTIN LUTHER KING')
        self.assertEqual(route_info10['service_type'], 'LOCAL')


class GetPeriodTests(unittest.TestCase):

    def setUp(self):
        tests_path = os.path.dirname(__file__)
        self.test_excel = os.path.join(tests_path, 'data/test_cmta_data.xls')

    def test_get_periods(self):
        excel_book = xlrd.open_workbook(filename=self.test_excel)
        worksheet = excel_book.sheet_by_name('Ridership by Route Weekday')
        periods = etl.get_periods(worksheet)
        self.assertEqual(periods['3']['day_of_week'], 'weekday')
        self.assertEqual(periods['3']['season'], 'spring')
        self.assertEqual(periods['3']['year'], 2012)
        timestamp_3 = APP_TIMEZONE.localize(datetime(year=2012, month=3, day=26))
        self.assertEqual(periods['3']['timestamp'], timestamp_3, msg=periods['3']['timestamp'])
        self.assertEqual(periods['8']['day_of_week'], 'weekday')
        self.assertEqual(periods['8']['season'], 'fall')
        self.assertEqual(periods['8']['year'], 2013)
        timestamp_8 = APP_TIMEZONE.localize(datetime(year=2013, month=9, day=30))
        self.assertEqual(periods['8']['timestamp'], timestamp_8)
        self.assertEqual(periods['10']['day_of_week'], 'weekday')
        self.assertEqual(periods['10']['season'], 'summer')
        self.assertEqual(periods['10']['year'], 2014)
        timestamp_10 = APP_TIMEZONE.localize(datetime(year=2014, month=6, day=30))
        self.assertEqual(periods['10']['timestamp'], timestamp_10)


class ExtractDayOfWeekTests(unittest.TestCase):

    def setUp(self):
        tests_path = os.path.dirname(__file__)
        self.test_excel = os.path.join(tests_path, 'data/test_cmta_data.xls')

    def test_weekday_extraction(self):
        excel_book = xlrd.open_workbook(filename=self.test_excel)
        worksheet = excel_book.sheet_by_name('Ridership by Route Weekday')
        day_of_week = etl.extract_day_of_week(3, 3, worksheet)
        self.assertEqual('weekday', day_of_week)
        no_day = etl.extract_day_of_week(10, 3, worksheet)
        self.assertIsNone(no_day)

    def test_saturday_extraction(self):
        excel_book = xlrd.open_workbook(filename=self.test_excel)
        worksheet = excel_book.sheet_by_name('Ridership by Route Saturday')
        day_of_week = etl.extract_day_of_week(3, 6, worksheet)
        self.assertEqual('saturday', day_of_week)
        no_day = etl.extract_day_of_week(10, 3, worksheet)
        self.assertIsNone(no_day)

    def test_sunday_extraction(self):
        excel_book = xlrd.open_workbook(filename=self.test_excel)
        worksheet = excel_book.sheet_by_name('Ridership by Route Sunday')
        day_of_week = etl.extract_day_of_week(3, 9, worksheet)
        self.assertEqual('sunday', day_of_week)
        no_day = etl.extract_day_of_week(10, 3, worksheet)
        self.assertIsNone(no_day)


class MergeRouteDataTests(unittest.TestCase):
    """
    Tests etl.merge_route_data function.
    """
    def test_route_number_redundancy(self):
        worksheet_result_1 = {
            'routes': [{'route_number': 1}],
        }
        worksheet_result_2 = {
            'routes': [
                {'route_number': 1},
                {'route_number': 2},
                {'route_number': 3}
            ],
        }
        results = [worksheet_result_1, worksheet_result_2]
        merged_data = etl.merge_route_data(results)
        self.assertEqual(len(list(merged_data.keys())), 3)
        self.assertTrue(set(merged_data.keys()).issuperset({1, 2, 3}))
        self.assertFalse(set(merged_data.keys()).issuperset({1, 2, 3, 4}))

    def test_duplicate_route_dict(self):
        route_info_1a = {
            'route_number': '1',
            'route_name': '1 - POPULAR ROUTE',
            'service_type': 'Local'
        }
        route_info_1b = {
            'route_number': '1',
            'route_name': '1 - POPULAR ROUTE',
            'service_type': 'Local'
        }
        route_info_2 = {
            'route_number': '2',
            'route_name': '2 - UNPOPULAR ROUTE',
            'service_type': 'Local'
        }
        route_info_3 = {
            'route_number': '3',
            'route_name': '3 - NEW ROUTE',
            'service_type': 'Express'
        }
        results = [{'routes': [route_info_1a, route_info_1b, route_info_2, route_info_3]}]
        merged_data = etl.merge_route_data(results)
        merged_keys = merged_data.keys()
        self.assertEqual(len(list(merged_keys)), 3)
        self.assertTrue('1' in merged_keys)
        self.assertTrue('2' in merged_keys)
        self.assertTrue('3' in merged_keys)


class ParseWorksheetRidershipTests(unittest.TestCase):
    """
    Tests etl.parse_worksheet_ridership function.
    """

    def setUp(self):
        tests_path = os.path.dirname(__file__)
        self.test_excel = os.path.join(tests_path, 'data/test_cmta_data_single.xls')
        excel_book = xlrd.open_workbook(filename=self.test_excel)
        self.worksheet = excel_book.sheet_by_name('Ridership by Route Weekday')
        self.periods = etl.get_periods(self.worksheet)
        self.engine = create_engine('sqlite:///:memory:')
        Session = sessionmaker()
        Session.configure(bind=self.engine)
        session = Session()
        models.Base.metadata.create_all(self.engine)
        self.session = session
        etl.update_route_info(self.test_excel,
                              session,
                              ['Ridership by Route Weekday'])

    def tearDown(self):
        models.Base.metadata.drop_all(self.engine)

    def test_parse(self):
        etl.parse_worksheet_ridership(self.worksheet, self.periods, models.DailyRidership,
                                      self.session)
        instances = self.session.query(models.DailyRidership).all()
        self.assertEqual(len(instances), 11)
        ridership_values = list()
        for daily_ridership in instances:
            ridership_values.append(daily_ridership.ridership)
        expected_set = {
            14041.3609132795,
            12794.7123015873,
            14117.8574082171,
            13633.8223760158,
            12875.3658424908,
            12459.0767904292,
            7949.55721234328,
            7000.32063492063,
            6740.593831168831,
            6227.303709,
            6037.422349
        }
        difference = expected_set.difference(set(ridership_values))
        self.assertEqual(len(list(difference)), 0)

    def test_parse_etl_report(self):
        report = models.ETLReport(creates=0, updates=0)
        etl.parse_worksheet_ridership(self.worksheet, self.periods, models.DailyRidership,
                                      self.session, report)
        instances = self.session.query(models.DailyRidership).all()
        self.assertEqual(len(instances), 11)
        ridership_values = list()
        for daily_ridership in instances:
            ridership_values.append(daily_ridership.ridership)
        expected_set = {
            14041.3609132795,
            12794.7123015873,
            14117.8574082171,
            13633.8223760158,
            12875.3658424908,
            12459.0767904292,
            7949.55721234328,
            7000.32063492063,
            6740.593831168831,
            6227.303709,
            6037.422349
        }
        difference = expected_set.difference(set(ridership_values))
        self.assertEqual(len(list(difference)), 0)
        self.assertEqual(report.creates, 11)
        self.assertEqual(report.updates, 0)

class StoreRouteTests(unittest.TestCase):
    """
    Tests etl.store_route function.
    """
    def setUp(self):
        self.timezone = pytz.timezone('America/Chicago')
        tests_path = os.path.dirname(__file__)
        self.test_excel = os.path.join(tests_path, 'data/test_cmta_data.xls')
        self.test_excel_with_updates = os.path.join(tests_path, 'data/test_cmta_updated_data.xls')
        self.engine = create_engine('sqlite:///:memory:')
        Session = sessionmaker()
        Session.configure(bind=self.engine)
        self.session = Session()
        models.Base.metadata.create_all(self.engine)
        self.worksheets = [
            "Ridership by Route Weekday",
            "Ridership by Route Saturday",
            "Ridership by Route Sunday",
            "Riders per Hour Weekday",
            "Riders Hour Saturday",
            "Riders per Hour Sunday"
        ]
        self.daily_worksheets = [
            "Ridership by Route Weekday",
            "Ridership by Route Saturday",
            "Ridership by Route Sunday",
        ]

    def tearDown(self):
        models.Base.metadata.drop_all(self.engine)

    def test_initial_store_route_info(self):
        for v in range(10):
            route_info = {
                'route_number': str(v),
                'route_name': '{0}-POPULAR ROUTE'.format(v),
                'service_type': 'Local'
            }
            etl.store_route(self.session, str(v), route_info)
        self.session.commit()
        for version in range(10):
            route = self.session.query(models.Route) \
                .filter_by(route_number=int(version)).one()
            self.assertEqual(route.route_name, '{0}-POPULAR ROUTE'.format(version))
            self.assertEqual(route.service_type, 'LOCAL')
        instances = self.session.query(models.Route).all()
        self.assertEqual(len(instances), 10)

    def test_store_route_info_update(self):
        route_info = {
            'route_number': '1',
            'route_name': '1-POPULAR ROUTE',
            'service_type': 'Local'
        }
        etl.store_route(self.session, '1', route_info)
        self.session.commit()
        new_route = self.session.query(models.Route).filter_by(route_number=1).one()
        self.assertEqual(new_route.route_name, '1-POPULAR ROUTE')
        self.assertEqual(new_route.service_type, 'LOCAL')
        new_route.route_name = '1 - REORGANIZED_ROUTE'
        self.session.commit()
        updated_route = self.session.query(models.Route).filter_by(route_number=1).one()
        self.assertEqual(updated_route.route_name, '1 - REORGANIZED_ROUTE')
        self.assertEqual(updated_route.service_type, 'LOCAL')


class UpdateRouteInfoTests(unittest.TestCase):
    """
    Tests etl.update_route_info function.
    """

    def setUp(self):
        tests_path = os.path.dirname(__file__)
        self.test_excel = os.path.join(tests_path, 'data/test_cmta_data.xls')
        self.test_excel_with_updates = os.path.join(tests_path, 'data/test_cmta_updated_data.xls')
        self.engine = create_engine('sqlite:///:memory:')
        Session = sessionmaker()
        Session.configure(bind=self.engine)
        self.session = Session()
        models.Base.metadata.create_all(self.engine)
        self.worksheets = [
            "Ridership by Route Weekday",
            "Ridership by Route Saturday",
            "Ridership by Route Sunday",
            "Riders per Hour Weekday",
            "Riders Hour Saturday",
            "Riders per Hour Sunday"
        ]
        self.daily_worksheets = [
            "Ridership by Route Weekday",
            "Ridership by Route Saturday",
            "Ridership by Route Sunday",
        ]

    def tearDown(self):
        models.Base.metadata.drop_all(self.engine)

    def test_update_route_info_bare_database(self):
        etl.update_route_info(self.test_excel, self.session, self.worksheets)
        instances = self.session.query(models.Route).all()
        self.assertEqual(len(instances), 88)
        route_1 = self.session.query(models.Route).filter_by(route_number=1).one()
        self.assertEqual(route_1.route_name, '1-NORTH LAMAR/SOUTH CONGRESS')
        self.assertEqual(route_1.service_type, 'LOCAL')
        route_20 = self.session.query(models.Route).filter_by(route_number=20).one()
        self.assertEqual(route_20.route_name, '20-MANOR RD/RIVERSIDE')
        self.assertEqual(route_20.service_type, 'LOCAL')
        route_100 = self.session.query(models.Route).filter_by(route_number=100).one()
        self.assertEqual(route_100.route_name, '100-AIRPORT FLYER')
        self.assertEqual(route_100.service_type, 'LIMITED/FLYER')
        route_271 = self.session.query(models.Route).filter_by(route_number=271).one()
        self.assertEqual(route_271.route_name, '271-DEL VALLE FLEX')
        self.assertEqual(route_271.service_type, 'FEEDER')
        route_350 = self.session.query(models.Route).filter_by(route_number=350).one()
        self.assertEqual(route_350.route_name, '350-AIRPORT BLVD')
        self.assertEqual(route_350.service_type, 'CROSSTOWN')
        route_412 = self.session.query(models.Route).filter_by(route_number=412).one()
        self.assertEqual(route_412.route_name, '412-EBUS/MAIN CAMPUS')
        self.assertEqual(route_412.service_type, 'SPECIAL SERVICES- EBUS')
        route_550 = self.session.query(models.Route).filter_by(route_number=550).one()
        self.assertEqual(route_550.route_name, '550-METRO RAIL RED LINE')
        self.assertEqual(route_550.service_type, 'METRORAIL')
        route_801 = self.session.query(models.Route).filter_by(route_number=803).one()
        self.assertEqual(route_801.route_name, 'METRORAPID 803 S LAMAR BURNET')
        self.assertEqual(route_801.service_type, 'METRORAPID')

    def test_update_route_info_report(self):
        report = etl.update_route_info(self.test_excel, self.session, self.worksheets)
        route_1 = self.session.query(models.Route).filter_by(route_number=1).one()
        self.assertEqual(route_1.route_name, '1-NORTH LAMAR/SOUTH CONGRESS')
        self.assertEqual(route_1.service_type, 'LOCAL')
        route_20 = self.session.query(models.Route).filter_by(route_number=20).one()
        self.assertEqual(route_20.route_name, '20-MANOR RD/RIVERSIDE')
        self.assertEqual(route_20.service_type, 'LOCAL')
        route_100 = self.session.query(models.Route).filter_by(route_number=100).one()
        self.assertEqual(route_100.route_name, '100-AIRPORT FLYER')
        self.assertEqual(route_100.service_type, 'LIMITED/FLYER')
        route_271 = self.session.query(models.Route).filter_by(route_number=271).one()
        self.assertEqual(route_271.route_name, '271-DEL VALLE FLEX')
        self.assertEqual(route_271.service_type, 'FEEDER')
        route_350 = self.session.query(models.Route).filter_by(route_number=350).one()
        self.assertEqual(route_350.route_name, '350-AIRPORT BLVD')
        self.assertEqual(route_350.service_type, 'CROSSTOWN')
        route_412 = self.session.query(models.Route).filter_by(route_number=412).one()
        self.assertEqual(route_412.route_name, '412-EBUS/MAIN CAMPUS')
        self.assertEqual(route_412.service_type, 'SPECIAL SERVICES- EBUS')
        route_550 = self.session.query(models.Route).filter_by(route_number=550).one()
        self.assertEqual(route_550.route_name, '550-METRO RAIL RED LINE')
        self.assertEqual(route_550.service_type, 'METRORAIL')
        route_801 = self.session.query(models.Route).filter_by(route_number=803).one()
        self.assertEqual(route_801.route_name, 'METRORAPID 803 S LAMAR BURNET')
        self.assertEqual(route_801.service_type, 'METRORAPID')
        self.assertEqual(report.creates, 88)
        self.assertEqual(report.updates, 0)
        self.assertEqual(report.total_models, 88)

    def test_update_route_info_with_existing_database(self):
        etl.update_route_info(self.test_excel, self.session, self.daily_worksheets)
        instances = self.session.query(models.Route).all()
        self.assertEqual(len(instances), 88)
        # now we update
        etl.update_route_info(self.test_excel_with_updates,
                              self.session,
                              self.daily_worksheets)
        route_1 = self.session.query(models.Route).filter_by(route_number=1).one()
        self.assertEqual(route_1.route_name, '1-NORTH LAMAR/SOUTH CONGRESS')
        self.assertEqual(route_1.service_type, 'LOCAL')
        route_10 = self.session.query(models.Route).filter_by(route_number=10).one()
        self.assertEqual(route_10.route_name, '10-SOUTH 1ST STREET/RED RIVER')
        self.assertEqual(route_10.service_type, 'UPDATED LOCAL')
        route_20 = self.session.query(models.Route).filter_by(route_number=20).one()
        self.assertEqual(route_20.route_name, '20-MANOR RD/RIVERSIDE')
        self.assertEqual(route_20.service_type, 'LOCAL')
        route_100 = self.session.query(models.Route).filter_by(route_number=100).one()
        self.assertEqual(route_100.route_name, '100-AIRPORT FLYER')
        self.assertEqual(route_100.service_type, 'LIMITED/FLYER')
        route_271 = self.session.query(models.Route).filter_by(route_number=271).one()
        self.assertEqual(route_271.route_name, '271-DEL VALLE FLEX')
        self.assertEqual(route_271.service_type, 'FEEDER')
        route_350 = self.session.query(models.Route).filter_by(route_number=350).one()
        self.assertEqual(route_350.route_name, '350-AIRPORT BLVD')
        self.assertEqual(route_350.service_type, 'CROSSTOWN')
        route_412 = self.session.query(models.Route).filter_by(route_number=412).one()
        self.assertEqual(route_412.route_name, '412-EBUS/MAIN CAMPUS')
        self.assertEqual(route_412.service_type, 'SPECIAL SERVICES- EBUS')
        route_550 = self.session.query(models.Route).filter_by(route_number=550).one()
        self.assertEqual(route_550.route_name, '550-METRO RAIL RED LINE')
        self.assertEqual(route_550.service_type, 'METRORAIL')
        route_801 = self.session.query(models.Route).filter_by(route_number=803).one()
        self.assertEqual(route_801.route_name, 'METRORAPID 803 S LAMAR BURNET')
        self.assertEqual(route_801.service_type, 'METRORAPID')

    def test_update_route_info_report_with_existing_database(self):
        first_report = etl.update_route_info(self.test_excel, self.session, self.daily_worksheets)
        instances = self.session.query(models.Route).all()
        self.assertEqual(len(instances), 88)
        self.assertEqual(first_report.creates, 88)
        self.assertEqual(first_report.updates, 0)
        self.assertEqual(first_report.total_models, 88)
        # now we update
        report = etl.update_route_info(self.test_excel_with_updates,
                                       self.session,
                                       self.daily_worksheets)
        route_1 = self.session.query(models.Route).filter_by(route_number=1).one()
        self.assertEqual(route_1.route_name, '1-NORTH LAMAR/SOUTH CONGRESS')
        self.assertEqual(route_1.service_type, 'LOCAL')
        route_10 = self.session.query(models.Route).filter_by(route_number=10).one()
        self.assertEqual(route_10.route_name, '10-SOUTH 1ST STREET/RED RIVER')
        self.assertEqual(route_10.service_type, 'UPDATED LOCAL')
        route_20 = self.session.query(models.Route).filter_by(route_number=20).one()
        self.assertEqual(route_20.route_name, '20-MANOR RD/RIVERSIDE')
        self.assertEqual(route_20.service_type, 'LOCAL')
        route_100 = self.session.query(models.Route).filter_by(route_number=100).one()
        self.assertEqual(route_100.route_name, '100-AIRPORT FLYER')
        self.assertEqual(route_100.service_type, 'LIMITED/FLYER')
        route_271 = self.session.query(models.Route).filter_by(route_number=271).one()
        self.assertEqual(route_271.route_name, '271-DEL VALLE FLEX')
        self.assertEqual(route_271.service_type, 'FEEDER')
        route_350 = self.session.query(models.Route).filter_by(route_number=350).one()
        self.assertEqual(route_350.route_name, '350-AIRPORT BLVD')
        self.assertEqual(route_350.service_type, 'CROSSTOWN')
        route_412 = self.session.query(models.Route).filter_by(route_number=412).one()
        self.assertEqual(route_412.route_name, '412-EBUS/MAIN CAMPUS')
        self.assertEqual(route_412.service_type, 'SPECIAL SERVICES- EBUS')
        route_550 = self.session.query(models.Route).filter_by(route_number=550).one()
        self.assertEqual(route_550.route_name, '550-METRO RAIL RED LINE')
        self.assertEqual(route_550.service_type, 'METRORAIL')
        route_801 = self.session.query(models.Route).filter_by(route_number=803).one()
        self.assertEqual(route_801.route_name, 'METRORAPID 803 S LAMAR BURNET')
        self.assertEqual(route_801.service_type, 'METRORAPID')
        self.assertEqual(report.creates, 0)
        self.assertEqual(report.updates, 88)
        self.assertEqual(report.total_models, 88)


class DeactivateCurrentPeriodTests(unittest.TestCase):

    def setUp(self):
        tests_path = os.path.dirname(__file__)
        self.test_excel = os.path.join(tests_path, 'data/test_cmta_data.xls')
        self.engine = create_engine('sqlite:///:memory:')
        Session = sessionmaker()
        Session.configure(bind=self.engine)
        self.session = Session()
        models.Base.metadata.create_all(self.engine)
        route = models.Route(route_number=1, route_name='POPULAR ROUTE',
                             service_type='LOCAL')
        self.session.add(route)
        self.session.commit()
        self.timestamp = etl.get_period_timestamp('weekday', 'spring', 2015)
        daily_ridership = models.DailyRidership(created_on=datetime.now(),
                                                is_current=True,
                                                day_of_week='weekday',
                                                season='spring',
                                                calendar_year=2015,
                                                ridership=700,
                                                route_id=route.id,
                                                season_timestamp=self.timestamp)
        self.session.add(daily_ridership)
        self.session.commit()

    def tearDown(self):
        models.Base.metadata.drop_all(self.engine)

    def test_deactivation(self):
        ridership_model = models.DailyRidership
        period = {
            'year': 2015,
            'season': 'spring',
            'timestamp': self.timestamp,
            'day_of_week': 'weekday'
        }
        etl.deactivate_current_period(1, period, ridership_model, self.session)
        self.session.commit()
        ridership = self.session.query(models.DailyRidership).one()
        self.assertFalse(ridership.is_current)


class HandleRidershipCellTests(unittest.TestCase):

    def setUp(self):
        tests_path = os.path.dirname(__file__)
        self.test_excel = os.path.join(tests_path, 'data/test_cmta_data.xls')
        self.engine = create_engine('sqlite:///:memory:')
        Session = sessionmaker()
        Session.configure(bind=self.engine)
        self.session = Session()
        models.Base.metadata.create_all(self.engine)
        route = models.Route(route_number=1, route_name='POPULAR ROUTE',
                             service_type='LOCAL')
        self.session.add(route)
        self.session.commit()
        self.timestamp = etl.get_period_timestamp('saturday', 'fall', 2013)
        daily_ridership = models.DailyRidership(created_on=datetime.now(),
                                                is_current=True,
                                                day_of_week='saturday',
                                                season='fall',
                                                calendar_year=2013,
                                                season_timestamp=self.timestamp,
                                                ridership=7000,
                                                route_id=route.id)
        hourly_ridership = models.ServiceHourRidership(created_on=datetime.now(),
                                                       is_current=True,
                                                       day_of_week='saturday',
                                                       season='fall',
                                                       season_timestamp=self.timestamp,
                                                       calendar_year=2013,
                                                       ridership=70.7,
                                                       route_id=route.id)
        self.session.add(daily_ridership)
        self.session.add(hourly_ridership)
        self.session.commit()

    def tearDown(self):
        models.Base.metadata.drop_all(self.engine)

    def test_handle_new_daily_ridership(self):
        excel_book = xlrd.open_workbook(filename=self.test_excel)
        worksheet = excel_book.sheet_by_name('Ridership by Route Saturday')
        ridership_cell = worksheet.cell(5, 8)
        period = {
            'year': 2013,
            'season': 'fall',
            'day_of_week': 'saturday',
            'timestamp': self.timestamp
        }
        etl.handle_ridership_cell(1, period, ridership_cell,
                                  models.DailyRidership, self.session)
        self.session.commit()
        ridership = self.session.query(models.DailyRidership)\
                        .filter_by(is_current=True).one()
        self.assertEqual(ridership.ridership, 10997.5717761557)
        old_ridership = self.session.query(models.DailyRidership) \
            .filter_by(is_current=False).one()
        self.assertEqual(old_ridership.ridership, float(7000))

    def test_handle_new_daily_ridership_with_report(self):
        excel_book = xlrd.open_workbook(filename=self.test_excel)
        worksheet = excel_book.sheet_by_name('Ridership by Route Saturday')
        ridership_cell = worksheet.cell(5, 8)
        period = {
            'year': 2013,
            'season': 'fall',
            'timestamp': self.timestamp,
            'day_of_week': 'saturday'
        }
        report = models.ETLReport(creates=0, updates=0)
        etl.handle_ridership_cell(1, period, ridership_cell,
                                  models.DailyRidership, self.session, report)
        self.session.commit()
        ridership = self.session.query(models.DailyRidership) \
            .filter_by(is_current=True).one()
        self.assertEqual(ridership.ridership, 10997.5717761557)
        old_ridership = self.session.query(models.DailyRidership) \
            .filter_by(is_current=False).one()
        self.assertEqual(old_ridership.ridership, float(7000))
        self.assertEqual(report.updates, 1)

    def test_handle_new_hourly_ridership(self):
        excel_book = xlrd.open_workbook(filename=self.test_excel)
        worksheet = excel_book.sheet_by_name('Riders Hour Saturday')
        ridership_cell = worksheet.cell(5, 8)
        period = {
            'year': 2013,
            'season': 'fall',
            'timestamp': self.timestamp,
            'day_of_week': 'saturday'
        }
        etl.handle_ridership_cell(1, period, ridership_cell,
                                  models.ServiceHourRidership, self.session)
        self.session.commit()
        ridership = self.session.query(models.ServiceHourRidership) \
            .filter_by(is_current=True).one()
        self.assertEqual(ridership.ridership, 39.8486808725975)
        old_ridership = self.session.query(models.ServiceHourRidership) \
            .filter_by(is_current=False).one()
        self.assertEqual(old_ridership.ridership, float(70.7))


class UpdateRidershipTests(unittest.TestCase):

    def setUp(self):
        tests_path = os.path.dirname(__file__)
        ini_config = os.path.join(tests_path, 'capmetrics_single.ini')
        config_parser = configparser.ConfigParser()
        # make parsing of config file names case-sensitive
        config_parser.optionxform = str
        config_parser.read(ini_config)
        self.config = cli.parse_capmetrics_configuration(config_parser)
        engine = create_engine(self.config['engine_url'])
        Session = sessionmaker()
        Session.configure(bind=engine)
        session = Session()
        models.Base.metadata.create_all(engine)
        self.session = session
        etl.update_route_info('./tests/data/test_cmta_data_single.xls',
                              session,
                              ['Ridership by Route Weekday'])

    def test_etl_reports(self):
        report = etl.update_ridership('./tests/data/test_cmta_data_single.xls',
                             ['Ridership by Route Weekday'],
                             models.DailyRidership,
                             self.session)
        self.assertEqual(report.total_models, 11)


class RunExcelETLTests(unittest.TestCase):

    def setUp(self):
        tests_path = os.path.dirname(__file__)
        ini_config = os.path.join(tests_path, 'capmetrics.ini')
        config_parser = configparser.ConfigParser()
        # make parsing of config file names case-sensitive
        config_parser.optionxform = str
        config_parser.read(ini_config)
        self.config = cli.parse_capmetrics_configuration(config_parser)
        engine = create_engine(self.config['engine_url'])
        Session = sessionmaker()
        Session.configure(bind=engine)
        session = Session()
        models.Base.metadata.create_all(engine)
        self.session = session

    def test_etl_reports(self):
        # This test takes a long time, as it perform the full etl task for a realistic file
        etl.run_excel_etl('./tests/data/test_cmta_data.xls', self.session, self.config)
        reports = self.session.query(models.ETLReport).all()
        self.assertTrue(len(reports), 3)


class CalibrateDayOfWeekTests(unittest.TestCase):
    """
    Tests etl.calibrate_day_of_week function.
    """

    def test_monday_before_first_of_month(self):
        # march 1, 2016 is a tuesday
        timestamp = datetime(year=2016, month=3, day=1)
        self.assertEqual(2, timestamp.isoweekday())
        timestamp = etl.calibrate_day_of_week(timestamp, 'weekday')
        self.assertEqual(1, timestamp.isoweekday())
        self.assertEqual(timestamp.month, 2)
        self.assertEqual(timestamp.day, 29)
        self.assertEqual(timestamp.year, 2016)

    def test_saturday_after_first_of_month(self):
        # march 1, 2016 is a tuesday
        timestamp = datetime(year=2016, month=3, day=1)
        self.assertEqual(2, timestamp.isoweekday())
        timestamp = etl.calibrate_day_of_week(timestamp, 'saturday')
        self.assertEqual(6, timestamp.isoweekday())
        self.assertEqual(timestamp.month, 3)
        self.assertEqual(timestamp.day, 5)
        self.assertEqual(timestamp.year, 2016)

    def test_saturday_before_first_of_month(self):
        # november 1, 2015 is a sunday
        timestamp = datetime(year=2015, month=11, day=1)
        self.assertEqual(7, timestamp.isoweekday())
        timestamp = etl.calibrate_day_of_week(timestamp, 'saturday')
        self.assertEqual(6, timestamp.isoweekday())
        self.assertEqual(timestamp.month, 10)
        self.assertEqual(timestamp.day, 31)
        self.assertEqual(timestamp.year, 2015)

    def test_sunday_after_first_of_month(self):
        # march 1, 2016 is a tuesday
        timestamp = datetime(year=2016, month=3, day=1)
        self.assertEqual(2, timestamp.isoweekday())
        timestamp = etl.calibrate_day_of_week(timestamp, 'sunday')
        self.assertEqual(7, timestamp.isoweekday())
        self.assertEqual(timestamp.month, 3)
        self.assertEqual(timestamp.day, 6)
        self.assertEqual(timestamp.year, 2016)

    def test_sunday_before_first_of_month(self):
        # june 1, 2015 is a monday
        timestamp = datetime(year=2015, month=6, day=1)
        self.assertEqual(1, timestamp.isoweekday())
        timestamp = etl.calibrate_day_of_week(timestamp, 'sunday')
        self.assertEqual(7, timestamp.isoweekday())
        self.assertEqual(timestamp.month, 6)
        self.assertEqual(timestamp.day, 7)
        self.assertEqual(timestamp.year, 2015)


class GetPeriodTimestampTests(unittest.TestCase):

    def test_spring_weekday(self):
        timestamp = etl.get_period_timestamp('weekday', 'spring', 2016)
        self.assertEqual(timestamp.isoweekday(), 1)
        self.assertEqual(timestamp.month, 3)
        self.assertEqual(timestamp.day, 28)
        self.assertEqual(timestamp.year, 2016)

    def test_spring_saturday(self):
        timestamp = etl.get_period_timestamp('saturday', 'spring', 2016)
        self.assertEqual(timestamp.isoweekday(), 6)
        self.assertEqual(timestamp.month, 4)
        self.assertEqual(timestamp.day, 2)
        self.assertEqual(timestamp.year, 2016)

    def test_spring_sunday(self):
        timestamp = etl.get_period_timestamp('sunday', 'spring', 2016)
        self.assertEqual(timestamp.isoweekday(), 7)
        self.assertEqual(timestamp.month, 4)
        self.assertEqual(timestamp.day, 3)
        self.assertEqual(timestamp.year, 2016)

    def test_summer_weekday(self):
        timestamp = etl.get_period_timestamp('weekday', 'summer', 2016)
        self.assertEqual(timestamp.isoweekday(), 1)
        self.assertEqual(timestamp.month, 6)
        self.assertEqual(timestamp.day, 27)
        self.assertEqual(timestamp.year, 2016)

    def test_summer_saturday(self):
        timestamp = etl.get_period_timestamp('saturday', 'summer', 2016)
        self.assertEqual(timestamp.isoweekday(), 6)
        self.assertEqual(timestamp.month, 7)
        self.assertEqual(timestamp.day, 2)
        self.assertEqual(timestamp.year, 2016)

    def test_summer_sunday(self):
        timestamp = etl.get_period_timestamp('sunday', 'summer', 2016)
        self.assertEqual(timestamp.isoweekday(), 7)
        self.assertEqual(timestamp.month, 7)
        self.assertEqual(timestamp.day, 3)
        self.assertEqual(timestamp.year, 2016)

    def test_fall_weekday(self):
        timestamp = etl.get_period_timestamp('weekday', 'fall', 2016)
        self.assertEqual(timestamp.isoweekday(), 1)
        self.assertEqual(timestamp.month, 9)
        self.assertEqual(timestamp.day, 26)
        self.assertEqual(timestamp.year, 2016)

    def test_fall_saturday(self):
        timestamp = etl.get_period_timestamp('saturday', 'fall', 2016)
        self.assertEqual(timestamp.isoweekday(), 6)
        self.assertEqual(timestamp.month, 10)
        self.assertEqual(timestamp.day, 1)
        self.assertEqual(timestamp.year, 2016)

    def test_fall_sunday(self):
        timestamp = etl.get_period_timestamp('sunday', 'fall', 2016)
        self.assertEqual(timestamp.isoweekday(), 7)
        self.assertEqual(timestamp.month, 10)
        self.assertEqual(timestamp.day, 2)
        self.assertEqual(timestamp.year, 2016)

    def test_winter_weekday(self):
        timestamp = etl.get_period_timestamp('weekday', 'winter', 2016)
        self.assertEqual(timestamp.isoweekday(), 1)
        self.assertEqual(timestamp.month, 12)
        self.assertEqual(timestamp.day, 28)
        self.assertEqual(timestamp.year, 2015)

    def test_winter_saturday(self):
        timestamp = etl.get_period_timestamp('saturday', 'winter', 2016)
        self.assertEqual(timestamp.isoweekday(), 6)
        self.assertEqual(timestamp.month, 1)
        self.assertEqual(timestamp.day, 2)
        self.assertEqual(timestamp.year, 2016)

    def test_winter_sunday(self):
        timestamp = etl.get_period_timestamp('sunday', 'winter', 2016)
        self.assertEqual(timestamp.isoweekday(), 7)
        self.assertEqual(timestamp.month, 1)
        self.assertEqual(timestamp.day, 3)
        self.assertEqual(timestamp.year, 2016)


class DeactivatePreviousSystemRidershipFacts(unittest.TestCase):

    def setUp(self):
        tests_path = os.path.dirname(__file__)
        ini_config = os.path.join(tests_path, 'capmetrics.ini')
        config_parser = configparser.ConfigParser()
        # make parsing of config file names case-sensitive
        config_parser.optionxform = str
        config_parser.read(ini_config)
        self.config = cli.parse_capmetrics_configuration(config_parser)
        engine = create_engine(self.config['engine_url'])
        Session = sessionmaker()
        Session.configure(bind=engine)
        session = Session()
        models.Base.metadata.create_all(engine)
        system_ridership_fact1 = models.SystemRidership(
            id=1,
            created_on=APP_TIMEZONE.localize(datetime.now()),
            is_active=True,
            day_of_week='weekday',
            season='winter',
            calendar_year=2015,
            service_type='bus',
            ridership=1000,
            season_timestamp=APP_TIMEZONE.localize(datetime(2015, 1, 1)))
        system_ridership_fact2 = models.SystemRidership(
            id=2,
            created_on=APP_TIMEZONE.localize(datetime.now()),
            is_active=True,
            day_of_week='weekday',
            season='fall',
            calendar_year=2015,
            service_type='bus',
            ridership=1000,
            season_timestamp=APP_TIMEZONE.localize(datetime(2015, 7, 1)))
        system_ridership_fact3 = models.SystemRidership(
            id=3,
            created_on=APP_TIMEZONE.localize(datetime.now()),
            is_active=True,
            day_of_week='weekday',
            season='spring',
            calendar_year=2015,
            service_type='bus',
            ridership=1000,
            season_timestamp=APP_TIMEZONE.localize(datetime(2015, 4, 1)))
        session.add_all([system_ridership_fact1,
                         system_ridership_fact2,
                         system_ridership_fact3])
        session.commit()
        self.session = session

    def test_deactivation(self):
        all_models = self.session.query(models.SystemRidership).all()
        self.assertEqual(len(all_models), 3)
        pre_actives = self.session.query(models.SystemRidership).filter_by(is_active=True).all()
        self.assertEqual(len(pre_actives), 3)
        etl.deactivate_previous_system_ridership_facts(self.session)
        post_actives = self.session.query(models.SystemRidership).filter_by(is_active=True).all()
        self.assertEqual(len(post_actives), 0)
        inactives = self.session.query(models.SystemRidership).filter_by(is_active=False).all()
        self.assertEqual(len(inactives), 3)





