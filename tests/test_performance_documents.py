import configparser
from datetime import datetime
import json
import os
import unittest
import pytz
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import xlrd
from capmetrics_etl import cli, etl, models, performance_documents

UTC_TIMEZONE = pytz.timezone('UTC')


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
            ridership=10000, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 1, 6)))
        system_ridership_fact_bus5 = models.SystemRidership(
            id=5, created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='saturday', season='spring', calendar_year=2015, service_type='bus',
            ridership=11000, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 4, 6)))
        system_ridership_fact_bus6 = models.SystemRidership(id=6,
            created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='saturday', season='summer', calendar_year=2015, service_type='bus',
            ridership=12000, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 7, 6)))
        system_ridership_fact_bus7 = models.SystemRidership(id=7,
            created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='sunday', season='winter', calendar_year=2015, service_type='bus',
            ridership=10000, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 1, 7)))
        system_ridership_fact_bus8 = models.SystemRidership(
            id=8, created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='sunday', season='spring', calendar_year=2015, service_type='bus',
            ridership=11000, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 4, 7)))
        system_ridership_fact_bus9 = models.SystemRidership(id=9,
            created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='sunday', season='summer', calendar_year=2015, service_type='bus',
            ridership=12000, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 7, 7)))
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
            day_of_week='weekday', season='fall', calendar_year=2015, service_type='rail',
            ridership=1200, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 7, 1)))
        system_ridership_fact_rail4 = models.SystemRidership(id=14,
            created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='saturday', season='winter', calendar_year=2015, service_type='rail',
            ridership=1090, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 1, 6)))
        system_ridership_fact_rail5 = models.SystemRidership(
            id=15, created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='saturday', season='spring', calendar_year=2015, service_type='rail',
            ridership=900, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 4, 6)))
        system_ridership_fact_rail6 = models.SystemRidership(id=16,
            created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='saturday', season='fall', calendar_year=2015, service_type='rail',
            ridership=1340, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 7, 6)))
        system_ridership_fact_rail7 = models.SystemRidership(id=17,
            created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='sunday', season='winter', calendar_year=2015, service_type='rail',
            ridership=1500, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 1, 7)))
        system_ridership_fact_rail8 = models.SystemRidership(
            id=18, created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='sunday', season='spring', calendar_year=2015, service_type='rail',
            ridership=1400, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 4, 7)))
        system_ridership_fact_rail9 = models.SystemRidership(id=19,
            created_on=UTC_TIMEZONE.localize(datetime.now()), is_active=True,
            day_of_week='sunday', season='fall', calendar_year=2015, service_type='rail',
            ridership=1300, measurement_timestamp=UTC_TIMEZONE.localize(datetime(2015, 7, 7)))
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

    def tearDown(self):
        models.Base.metadata.drop_all(self.engine)

    def test_document(self):
        performance_documents.update_system_trends_document(self.session)
        pd = self.session.query(models.PerformanceDocument)\
                      .filter_by(name='system-trends').one()
        document = json.loads(pd.document)
        self.assertEqual(document['data'][0]['attributes']['updated-on'], 'X0')


