import datetime
import os
import unittest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import xlrd
from capmetrics_etl import config, etl, models


class RouteInfoTests(unittest.TestCase):

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
        self.assertEqual(periods['8']['day_of_week'], 'weekday')
        self.assertEqual(periods['8']['season'], 'fall')
        self.assertEqual(periods['8']['year'], 2013)
        self.assertEqual(periods['10']['day_of_week'], 'weekday')
        self.assertEqual(periods['10']['season'], 'summer')
        self.assertEqual(periods['10']['year'], 2014)


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


class RouteInfoUpdateTests(unittest.TestCase):

    def setUp(self):
        tests_path = os.path.dirname(__file__)
        self.test_excel = os.path.join(tests_path, 'data/test_cmta_data.xls')
        self.engine = create_engine('sqlite:///:memory:')
        Session = sessionmaker()
        Session.configure(bind=self.engine)
        self.session = Session()
        models.Base.metadata.create_all(self.engine)

    def tearDown(self):
        models.Base.metadata.drop_all(self.engine)

    def test_store_route_info(self):
        for v in range(10):
            route_info = {
                'route_number': str(v),
                'route_name': '{0}-POPULAR ROUTE'.format(v),
                'service_type': 'Local'
            }
            etl.store_route(self.session, str(v), route_info)
        self.session.commit()
        for version in range(10):
            route = self.session.query(models.Route)\
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

    def test_merge_data(self):
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

    def test_update_route_info(self):
        etl.update_route_info(self.test_excel, self.session, config.WORKSHEETS)
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
        daily_ridership = models.DailyRidership(created_on=datetime.datetime.now(),
                                                current=True,
                                                day_of_week='weekday',
                                                season='spring',
                                                year=2015,
                                                ridership=700,
                                                route_id=route.id)
        self.session.add(daily_ridership)
        self.session.commit()

    def tearDown(self):
        models.Base.metadata.drop_all(self.engine)

    def test_deactivation(self):
        ridership_model = models.DailyRidership
        period = {
            'year': 2015,
            'season': 'spring',
            'day_of_week': 'weekday'
        }
        etl.deactivate_current_period(1, period, ridership_model, self.session)
        self.session.commit()
        ridership = self.session.query(models.DailyRidership).one()
        self.assertFalse(ridership.current)


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
        daily_ridership = models.DailyRidership(created_on=datetime.datetime.now(),
                                                current=True,
                                                day_of_week='saturday',
                                                season='fall',
                                                year=2013,
                                                ridership=7000,
                                                route_id=route.id)
        hourly_ridership = models.HourlyRidership(created_on=datetime.datetime.now(),
                                                  current=True,
                                                  day_of_week='saturday',
                                                  season='fall',
                                                  year=2013,
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
        ridership_cell = worksheet.cell(5,8)
        period = {
            'year': 2013,
            'season': 'fall',
            'day_of_week': 'saturday'
        }
        etl.handle_ridership_cell(1, period, ridership_cell,
                                  models.DailyRidership, self.session)
        self.session.commit()
        ridership = self.session.query(models.DailyRidership)\
                        .filter_by(current=True).one()
        self.assertEqual(ridership.ridership, 10997.5717761557)
        old_ridership = self.session.query(models.DailyRidership) \
            .filter_by(current=False).one()
        self.assertEqual(old_ridership.ridership, float(7000))

    def test_handle_new_hourly_ridership(self):
        excel_book = xlrd.open_workbook(filename=self.test_excel)
        worksheet = excel_book.sheet_by_name('Riders Hour Saturday')
        ridership_cell = worksheet.cell(5, 8)
        period = {
            'year': 2013,
            'season': 'fall',
            'day_of_week': 'saturday'
        }
        etl.handle_ridership_cell(1, period, ridership_cell,
                                  models.HourlyRidership, self.session)
        self.session.commit()
        ridership = self.session.query(models.HourlyRidership) \
            .filter_by(current=True).one()
        self.assertEqual(ridership.ridership, 39.8486808725975)
        old_ridership = self.session.query(models.HourlyRidership) \
            .filter_by(current=False).one()
        self.assertEqual(old_ridership.ridership, float(70.7))


