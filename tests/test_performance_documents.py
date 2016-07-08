import configparser
from datetime import datetime
import json
import os
import unittest
import pytz
import re
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import xlrd
from capmetrics_etl import cli, etl, models
from capmetrics_etl import performance_documents as perfdocs

UTC_TIMEZONE = pytz.timezone('UTC')


class BuildRouteDocumentTests(unittest.TestCase):

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
        etl.update_ridership('./tests/data/test_cmta_data_single.xls',
                             ['Ridership by Route Weekday'],
                             models.DailyRidership,
                             self.session)

        etl.update_ridership('./tests/data/test_cmta_data_single.xls',
                             ['Riders per Hour Weekday'],
                             models.ServiceHourRidership,
                             self.session)

    def tearDown(self):
        models.Base.metadata.drop_all(self.engine)

    def test_build(self):
        route = self.session.query(models.Route).filter_by(id=1).one()
        document = json.loads(perfdocs.build_route_document(route))
        self.assertEqual(document['data']['type'], 'routes')
        self.assertEqual(document['data']['id'], '1')
        attributes = document['data']['attributes']
        self.assertEqual(attributes['route-number'], 1)
        self.assertEqual(attributes['route-name'], '1-NORTH LAMAR/SOUTH CONGRESS')
        self.assertEqual(attributes['service-type'], 'LOCAL')
        self.assertFalse(attributes['is-high-ridership'])
        relationships = document['data']['relationships']
        self.assertEqual(len(relationships['daily-riderships']['data']), 11)
        daily_ridership = relationships['daily-riderships']['data'][0]
        self.assertTrue('id' in daily_ridership)
        self.assertTrue('type' in daily_ridership)
        self.assertEqual(len(relationships['service-hour-riderships']['data']), 11)
        service_hour_ridership = relationships['service-hour-riderships']['data'][0]
        self.assertTrue('id' in service_hour_ridership)
        self.assertTrue('type' in service_hour_ridership)
        self.assertEqual(len(document['included']), 22, msg=document['included'])
        resource_object = document['included'][0]
        self.assertTrue('id' in resource_object)
        self.assertTrue('type' in resource_object)
        ro_attributes = resource_object['attributes']
        # check for the UTC 'Z' zulu time indicator
        pattern = r'[0-9\:\-\.T]+Z$'
        zulu_regex = re.compile(pattern)
        self.assertTrue(zulu_regex.match(ro_attributes['measurement-timestamp']), msg=ro_attributes)
        self.assertTrue('created-on' in ro_attributes)
        self.assertTrue('is-current' in ro_attributes)
        self.assertTrue('day-of-week' in ro_attributes)
        self.assertTrue('season' in ro_attributes)
        self.assertTrue('calendar-year' in ro_attributes)
        self.assertTrue('ridership' in ro_attributes)
        ro_relationships = resource_object['relationships']
        self.assertEqual(ro_relationships['route']['data']['id'], '1')
        self.assertEqual(ro_relationships['route']['data']['type'], 'routes')


class TransformRidershipCollectionTests(unittest.TestCase):

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
        etl.update_ridership('./tests/data/test_cmta_data_single.xls',
                             ['Ridership by Route Weekday'],
                             models.DailyRidership,
                             self.session)

    def tearDown(self):
        models.Base.metadata.drop_all(self.engine)

    def test_transformation(self):
        daily_riderships = self.session.query(models.DailyRidership).all()
        included = []
        resource_identfiers = perfdocs.transform_ridership_collection(daily_riderships,
                                                                      'daily-riderships',
                                                                      1,
                                                                      included)
        self.assertEqual(len(daily_riderships), 11)
        self.assertEqual(len(resource_identfiers), 11, msg=resource_identfiers)
        resource_identifier = resource_identfiers[0]
        self.assertTrue('id' in resource_identifier)
        self.assertTrue('type' in resource_identifier)
        self.assertEqual(resource_identifier['type'], 'daily-riderships')


class UpdateRouteSparklinesTests(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        Session = sessionmaker()
        Session.configure(bind=self.engine)
        session = Session()
        models.Base.metadata.create_all(self.engine)
        self.session = session
        route1 = models.Route(id=1, route_number=1,
                              route_name='SERVICIO UNO',
                              service_type='LOCAL')
        route2 = models.Route(id=2, route_number=2,
                              route_name='SERVICIO DOS',
                              service_type='LOCAL')
        route3 = models.Route(id=3, route_number=3,
                              route_name='SERVICIO TRES',
                              service_type='LOCAL')
        session.add_all([route1, route2, route3])

        # spring season for route 1
        daily_ridership_r1_spring_1 = models.DailyRidership(created_on=datetime.now(tz=UTC_TIMEZONE),
                                                            is_current=True,
                                                            day_of_week='weekday',
                                                            season='spring',
                                                            calendar_year=2010,
                                                            measurement_timestamp=datetime(2010, 4, 1,
                                                                                           tzinfo=UTC_TIMEZONE),
                                                            ridership=7000,
                                                            route_id=1)
        daily_ridership_r1_spring_2 = models.DailyRidership(created_on=datetime.now(tz=UTC_TIMEZONE),
                                                            is_current=True,
                                                            day_of_week='saturday',
                                                            season='spring',
                                                            calendar_year=2010,
                                                            measurement_timestamp=datetime(2010, 4, 1,
                                                                                           tzinfo=UTC_TIMEZONE),
                                                            ridership=1100,
                                                            route_id=1)
        # summer season for route 1
        daily_ridership_r1_summer_1 = models.DailyRidership(created_on=datetime.now(tz=UTC_TIMEZONE),
                                                            is_current=True,
                                                            day_of_week='weekday',
                                                            season='summer',
                                                            calendar_year=2010,
                                                            measurement_timestamp=datetime(2010, 7, 1,
                                                                                           tzinfo=UTC_TIMEZONE),
                                                            ridership=8000,
                                                            route_id=1)
        daily_ridership_r1_summer_2 = models.DailyRidership(created_on=datetime.now(tz=UTC_TIMEZONE),
                                                            is_current=True,
                                                            day_of_week='saturday',
                                                            season='summer',
                                                            calendar_year=2010,
                                                            measurement_timestamp=datetime(2010, 7, 1,
                                                                                           tzinfo=UTC_TIMEZONE),
                                                            ridership=1200,
                                                            route_id=1)
        # fall season for route 1
        daily_ridership_r1_fall_1 = models.DailyRidership(created_on=datetime.now(tz=UTC_TIMEZONE),
                                                            is_current=True,
                                                            day_of_week='weekday',
                                                            season='fall',
                                                            calendar_year=2010,
                                                            measurement_timestamp=datetime(2010, 10, 1,
                                                                                           tzinfo=UTC_TIMEZONE),
                                                            ridership=8500,
                                                            route_id=1)
        daily_ridership_r1_fall_2 = models.DailyRidership(created_on=datetime.now(tz=UTC_TIMEZONE),
                                                            is_current=True,
                                                            day_of_week='saturday',
                                                            season='fall',
                                                            calendar_year=2010,
                                                            measurement_timestamp=datetime(2010, 10, 1,
                                                                                           tzinfo=UTC_TIMEZONE),
                                                            ridership=1300,
                                                            route_id=1)


        # spring season for route 2
        daily_ridership_r2_spring_1 = models.DailyRidership(created_on=datetime.now(tz=UTC_TIMEZONE),
                                                            is_current=True,
                                                            day_of_week='weekday',
                                                            season='spring',
                                                            calendar_year=2010,
                                                            measurement_timestamp=datetime(2010, 4, 1,
                                                                                           tzinfo=UTC_TIMEZONE),
                                                            ridership=1726,
                                                            route_id=2)
        daily_ridership_r2_spring_2 = models.DailyRidership(created_on=datetime.now(tz=UTC_TIMEZONE),
                                                            is_current=True,
                                                            day_of_week='saturday',
                                                            season='spring',
                                                            calendar_year=2010,
                                                            measurement_timestamp=datetime(2010, 4, 1,
                                                                                           tzinfo=UTC_TIMEZONE),
                                                            ridership=1100,
                                                            route_id=2)
        # summer season for route 2
        daily_ridership_r2_summer_1 = models.DailyRidership(created_on=datetime.now(tz=UTC_TIMEZONE),
                                                            is_current=True,
                                                            day_of_week='weekday',
                                                            season='summer',
                                                            calendar_year=2010,
                                                            measurement_timestamp=datetime(2010, 7, 1,
                                                                                           tzinfo=UTC_TIMEZONE),
                                                            ridership=8000,
                                                            route_id=2)
        daily_ridership_r2_summer_2 = models.DailyRidership(created_on=datetime.now(tz=UTC_TIMEZONE),
                                                            is_current=True,
                                                            day_of_week='saturday',
                                                            season='summer',
                                                            calendar_year=2010,
                                                            measurement_timestamp=datetime(2010, 7, 1,
                                                                                           tzinfo=UTC_TIMEZONE),
                                                            ridership=1200,
                                                            route_id=2)
        # fall season for route 2
        daily_ridership_r2_fall_1 = models.DailyRidership(created_on=datetime.now(tz=UTC_TIMEZONE),
                                                          is_current=True,
                                                          day_of_week='weekday',
                                                          season='fall',
                                                          calendar_year=2010,
                                                          measurement_timestamp=datetime(2010, 10, 1,
                                                                                         tzinfo=UTC_TIMEZONE),
                                                          ridership=500,
                                                          route_id=2)
        daily_ridership_r2_fall_2 = models.DailyRidership(created_on=datetime.now(tz=UTC_TIMEZONE),
                                                          is_current=True,
                                                          day_of_week='saturday',
                                                          season='fall',
                                                          calendar_year=2010,
                                                          measurement_timestamp=datetime(2010, 10, 1,
                                                                                         tzinfo=UTC_TIMEZONE),
                                                          ridership=300,
                                                          route_id=2)
        # spring season for route 3
        daily_ridership_r3_spring_1 = models.DailyRidership(created_on=datetime.now(tz=UTC_TIMEZONE),
                                                            is_current=True,
                                                            day_of_week='weekday',
                                                            season='spring',
                                                            calendar_year=2010,
                                                            measurement_timestamp=datetime(2010, 4, 1,
                                                                                           tzinfo=UTC_TIMEZONE),
                                                            ridership=5550,
                                                            route_id=3)
        daily_ridership_r3_spring_2 = models.DailyRidership(created_on=datetime.now(tz=UTC_TIMEZONE),
                                                            is_current=True,
                                                            day_of_week='saturday',
                                                            season='spring',
                                                            calendar_year=2010,
                                                            measurement_timestamp=datetime(2010, 4, 1,
                                                                                           tzinfo=UTC_TIMEZONE),
                                                            ridership=1100,
                                                            route_id=3)
        # summer season for route 3
        daily_ridership_r3_summer_1 = models.DailyRidership(created_on=datetime.now(tz=UTC_TIMEZONE),
                                                            is_current=True,
                                                            day_of_week='weekday',
                                                            season='summer',
                                                            calendar_year=2010,
                                                            measurement_timestamp=datetime(2010, 7, 1,
                                                                                           tzinfo=UTC_TIMEZONE),
                                                            ridership=8000,
                                                            route_id=3)
        daily_ridership_r3_summer_2 = models.DailyRidership(created_on=datetime.now(tz=UTC_TIMEZONE),
                                                            is_current=True,
                                                            day_of_week='saturday',
                                                            season='summer',
                                                            calendar_year=2010,
                                                            measurement_timestamp=datetime(2010, 7, 1,
                                                                                           tzinfo=UTC_TIMEZONE),
                                                            ridership=1200,
                                                            route_id=3)
        # fall season for route 3
        daily_ridership_r3_fall_1 = models.DailyRidership(created_on=datetime.now(tz=UTC_TIMEZONE),
                                                          is_current=True,
                                                          day_of_week='weekday',
                                                          season='fall',
                                                          calendar_year=2010,
                                                          measurement_timestamp=datetime(2010, 10, 1,
                                                                                         tzinfo=UTC_TIMEZONE),
                                                          ridership=9300,
                                                          route_id=3)
        daily_ridership_r3_fall_2 = models.DailyRidership(created_on=datetime.now(tz=UTC_TIMEZONE),
                                                          is_current=True,
                                                          day_of_week='saturday',
                                                          season='fall',
                                                          calendar_year=2010,
                                                          measurement_timestamp=datetime(2010, 10, 1,
                                                                                         tzinfo=UTC_TIMEZONE),
                                                          ridership=3330,
                                                          route_id=3)
        session.add_all([
            daily_ridership_r1_fall_1, daily_ridership_r3_fall_2,
            daily_ridership_r2_fall_1, daily_ridership_r2_summer_2,
            daily_ridership_r1_spring_1, daily_ridership_r3_summer_2,
            daily_ridership_r3_fall_1, daily_ridership_r1_fall_2,
            daily_ridership_r1_spring_2, daily_ridership_r1_summer_1,
            daily_ridership_r1_summer_2, daily_ridership_r3_summer_1,
            daily_ridership_r3_spring_1, daily_ridership_r3_spring_2,
            daily_ridership_r2_fall_2, daily_ridership_r2_spring_1,
            daily_ridership_r2_spring_2, daily_ridership_r2_summer_1
        ])
        session.commit()
        perfdocs.update_route_sparklines(self.session)
        self.pd = self.session.query(models.PerformanceDocument) \
            .filter_by(name='ridership-sparklines').one()
        self.document = json.loads(self.pd.document)

    def tearDown(self):
        models.Base.metadata.drop_all(self.engine)

    def test_dates_sorted(self):
        top_ridership = self.document[0]
        weekly_ridership = top_ridership['data']
        bottom_ridership = self.document[2]
        weekly_ridership2 = bottom_ridership['data']
        self.assertEqual(weekly_ridership[0]['date'], '2010-03-29T05:00:00Z')
        self.assertEqual(weekly_ridership[1]['date'], '2010-06-28T05:00:00Z')
        self.assertEqual(weekly_ridership[2]['date'], '2010-09-27T05:00:00Z')
        self.assertEqual(weekly_ridership2[0]['date'], '2010-03-29T05:00:00Z')
        self.assertEqual(weekly_ridership2[1]['date'], '2010-06-28T05:00:00Z')
        self.assertEqual(weekly_ridership2[2]['date'], '2010-09-27T05:00:00Z')

    def test_latest_value_sort(self):
        # highest ridership latest value is route 3, then 1, then 2
        top_ridership = self.document[0]
        bottom_ridership = self.document[2]
        self.assertEqual(top_ridership['routeName'], 'SERVICIO TRES')
        self.assertEqual(bottom_ridership['routeName'], 'SERVICIO DOS')

    def test_weekly_aggregation_calculation(self):
        top_ridership = self.document[0]
        bottom_ridership = self.document[2]
        self.assertEqual(top_ridership['data'][2]['ridership'], 49830, msg=json.dumps(self.document))
        self.assertEqual(top_ridership['data'][0]['ridership'], 28850)
        self.assertEqual(bottom_ridership['data'][2]['ridership'], 2800)
        self.assertEqual(bottom_ridership['data'][0]['ridership'], 9730)


class UpdateSystemTrendsDocumentTests(unittest.TestCase):

    def setUp(self):
        """
        Tested data

        + ----------------------------------------------+
        | Bus                                           |
        + ----------------------------------------------+
        | season/day  |   winter  |  spring  |  summer  |
        |-------------+-----------+----------+----------+
        | weekday     | 10,000    | 11,000   | 12,000   |
        |-------------+-----------+----------+----------+
        | saturday    | 10,000    | 11,000   | 12,000   |
        |-------------+-----------+----------+----------+
        | sunday      | 10,000    | 11,000   | 12,000   |
        +-------------+-----------+----------+----------+
        | Total       | 70,000    | 77,000   | 84,000   |
        +-------------+-----------+----------+----------+


        + ----------------------------------------------+
        | Rail                                          |
        + ----------------------------------------------+
        | season/day  |   winter  |  spring  |  fall    |
        |-------------+-----------+----------+----------+
        | weekday     | 1,060     |  800     |  1,200   |
        |-------------+-----------+----------+----------+
        | saturday    | 1,090     |  900     |  1,340   |
        |-------------+-----------+----------+----------+
        | sunday      | 1,500     | 1,400    |  1,300   |
        +-------------+-----------+----------+----------+
        | Total       | 7,890     | 6,300    |  8,640   |
        +-------------+-----------+----------+----------+

        """
        tests_path = os.path.dirname(__file__)
        ini_config = os.path.join(tests_path, 'capmetrics.ini')
        config_parser = configparser.ConfigParser()
        # make parsing of config file names case-sensitive
        config_parser.optionxform = str
        config_parser.read(ini_config)
        self.config = cli.parse_capmetrics_configuration(config_parser)
        self.engine = create_engine(self.config['engine_url'])
        Session = sessionmaker()
        Session.configure(bind=self.engine)
        session = Session()
        models.Base.metadata.create_all(self.engine)
        # 3 bus data points per season and day of week
        system_ridership_fact_bus1 = models.SystemRidership(id=1, created_on=UTC_TIMEZONE.localize(datetime.now()),
            is_active=True, day_of_week='weekday', season='winter', calendar_year=2015, service_type='bus',
            ridership=10000, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 1, 1)))
        system_ridership_fact_bus2 = models.SystemRidership(
            id=2, created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='weekday', season='spring', calendar_year=2015, service_type='bus',
            ridership=11000, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 4, 1)))
        system_ridership_fact_bus3 = models.SystemRidership(
            id=3, created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='weekday', season='summer', calendar_year=2015, service_type='bus',
            ridership=12000, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 7, 1)))
        system_ridership_fact_bus4 = models.SystemRidership(id=4, created_on=UTC_TIMEZONE.localize(datetime.now()),
            is_active=True, day_of_week='saturday', season='winter', calendar_year=2015, service_type='bus',
            ridership=10000, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 1, 1)))
        system_ridership_fact_bus5 = models.SystemRidership(
            id=5, created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='saturday', season='spring', calendar_year=2015, service_type='bus',
            ridership=11000, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 4, 1)))
        system_ridership_fact_bus6 = models.SystemRidership(id=6,
            created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='saturday', season='summer', calendar_year=2015, service_type='bus',
            ridership=12000, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 7, 1)))
        system_ridership_fact_bus7 = models.SystemRidership(id=7,
            created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='sunday', season='winter', calendar_year=2015, service_type='bus',
            ridership=10000, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 1, 1)))
        system_ridership_fact_bus8 = models.SystemRidership(
            id=8, created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='sunday', season='spring', calendar_year=2015, service_type='bus',
            ridership=11000, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 4, 1)))
        system_ridership_fact_bus9 = models.SystemRidership(id=9,
            created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='sunday', season='summer', calendar_year=2015, service_type='bus',
            ridership=12000, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 7, 1)))
        # 3 rail data points per season and day of week
        system_ridership_fact_rail1 = models.SystemRidership(id=11,
            created_on=UTC_TIMEZONE.localize(datetime.now()),
            is_active=True, day_of_week='weekday', season='winter', calendar_year=2015, service_type='rail',
            ridership=1060, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 1, 1)))
        system_ridership_fact_rail2 = models.SystemRidership(
            id=12, created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='weekday', season='spring', calendar_year=2015, service_type='rail',
            ridership=800, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 4, 1)))
        system_ridership_fact_rail3 = models.SystemRidership(
            id=13, created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='weekday', season='summer', calendar_year=2015, service_type='rail',
            ridership=1200, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 7, 1)))
        system_ridership_fact_rail4 = models.SystemRidership(id=14,
            created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='saturday', season='winter', calendar_year=2015, service_type='rail',
            ridership=1090, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 1, 1)))
        system_ridership_fact_rail5 = models.SystemRidership(
            id=15, created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='saturday', season='spring', calendar_year=2015, service_type='rail',
            ridership=900, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 4, 1)))
        system_ridership_fact_rail6 = models.SystemRidership(id=16,
            created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='saturday', season='summer', calendar_year=2015, service_type='rail',
            ridership=1340, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 7, 1)))
        system_ridership_fact_rail7 = models.SystemRidership(id=17,
            created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='sunday', season='winter', calendar_year=2015, service_type='rail',
            ridership=1500, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 1, 1)))
        system_ridership_fact_rail8 = models.SystemRidership(
            id=18, created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='sunday', season='spring', calendar_year=2015, service_type='rail',
            ridership=1400, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 4, 1)))
        system_ridership_fact_rail9 = models.SystemRidership(id=19,
            created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='sunday', season='summer', calendar_year=2015, service_type='rail',
            ridership=1300, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 7, 1)))
        session.add_all([system_ridership_fact_bus1,
                         system_ridership_fact_bus2,
                         system_ridership_fact_bus3,
                         system_ridership_fact_bus4,
                         system_ridership_fact_bus5,
                         system_ridership_fact_bus6,
                         system_ridership_fact_bus7,
                         system_ridership_fact_bus8,
                         system_ridership_fact_bus9,
                         system_ridership_fact_rail1,
                         system_ridership_fact_rail2,
                         system_ridership_fact_rail3,
                         system_ridership_fact_rail4,
                         system_ridership_fact_rail5,
                         system_ridership_fact_rail6,
                         system_ridership_fact_rail7,
                         system_ridership_fact_rail8,
                         system_ridership_fact_rail9])
        session.commit()
        etl.update_system_trends(session)
        self.session = session
        perfdocs.update_system_trends_document(self.session)
        self.pd = self.session.query(models.PerformanceDocument) \
            .filter_by(name='system-trends').one()
        self.document = json.loads(self.pd.document)

    def tearDown(self):
        models.Base.metadata.drop_all(self.engine)

    def test_utc_zulu_added(self):
        iso_string = self.document['data'][0]['attributes']['updated-on']
        pattern = r'[0-9\:\-\.T]+Z$'
        zulu_regex = re.compile(pattern)
        self.assertTrue(zulu_regex.match(iso_string), msg=iso_string)

    def test_resource_type(self):
        resource_type = self.document['data'][0]['type']
        self.assertEqual(resource_type, 'system-trends')

    def test_includes_attribute_fields(self):
        attributes = self.document['data'][0]['attributes']
        self.assertTrue('updated-on' in attributes)
        self.assertTrue('trend' in attributes)
        self.assertTrue('service-type' in attributes)

    def test_trend_field_format(self):
        bus_trend_string = ''
        rail_trend_string = ''
        for system_trend in self.document['data']:
            if system_trend['attributes']['service-type'] == 'RAIL':
                rail_trend_string = system_trend['attributes']['trend']
            else:
                bus_trend_string = system_trend['attributes']['trend']
        bus_trend = json.loads(bus_trend_string)
        rail_trend = json.loads(rail_trend_string)
        self.assertEqual(len(bus_trend), 3)
        self.assertEqual(len(rail_trend), 3)
        self.assertEqual(bus_trend[0][0], '2014-12-29T06:00:00+00:00')
        self.assertEqual(bus_trend[1][0], '2015-03-30T05:00:00+00:00')
        self.assertEqual(bus_trend[2][0], '2015-06-29T05:00:00+00:00')
        self.assertEqual(bus_trend[0][1], 70000, msg=bus_trend_string)
        self.assertEqual(bus_trend[1][1], 77000)
        self.assertEqual(bus_trend[2][1], 84000)
        self.assertEqual(rail_trend[0][0], '2014-12-29T06:00:00+00:00')
        self.assertEqual(rail_trend[1][0], '2015-03-30T05:00:00+00:00')
        self.assertEqual(rail_trend[2][0], '2015-06-29T05:00:00+00:00')
        self.assertEqual(rail_trend[0][1], 7890, msg=rail_trend_string)
        self.assertEqual(rail_trend[1][1], 6300)
        self.assertEqual(rail_trend[2][1], 8640)
