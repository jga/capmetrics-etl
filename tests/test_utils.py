from datetime import datetime
import unittest
import pytz
from capmetrics_etl import utils


APP_TIMEZONE = pytz.timezone('America/Chicago')
UTC_TIMEZONE = pytz.timezone('UTC')

class GetPeriodTimestampTests(unittest.TestCase):

    def test_spring_weekday(self):
        timestamp = utils.get_period_timestamp('weekday', 'spring', 2016)
        self.assertEqual(timestamp.isoweekday(), 1)
        self.assertEqual(timestamp.month, 3)
        self.assertEqual(timestamp.day, 28)
        self.assertEqual(timestamp.year, 2016)

    def test_spring_saturday(self):
        timestamp = utils.get_period_timestamp('saturday', 'spring', 2016)
        self.assertEqual(timestamp.isoweekday(), 6)
        self.assertEqual(timestamp.month, 4)
        self.assertEqual(timestamp.day, 2)
        self.assertEqual(timestamp.year, 2016)

    def test_spring_sunday(self):
        timestamp = utils.get_period_timestamp('sunday', 'spring', 2016)
        self.assertEqual(timestamp.isoweekday(), 7)
        self.assertEqual(timestamp.month, 4)
        self.assertEqual(timestamp.day, 3)
        self.assertEqual(timestamp.year, 2016)

    def test_summer_weekday(self):
        timestamp = utils.get_period_timestamp('weekday', 'summer', 2016)
        self.assertEqual(timestamp.isoweekday(), 1)
        self.assertEqual(timestamp.month, 6)
        self.assertEqual(timestamp.day, 27)
        self.assertEqual(timestamp.year, 2016)

    def test_summer_saturday(self):
        timestamp = utils.get_period_timestamp('saturday', 'summer', 2016)
        self.assertEqual(timestamp.isoweekday(), 6)
        self.assertEqual(timestamp.month, 7)
        self.assertEqual(timestamp.day, 2)
        self.assertEqual(timestamp.year, 2016)

    def test_summer_sunday(self):
        timestamp = utils.get_period_timestamp('sunday', 'summer', 2016)
        self.assertEqual(timestamp.isoweekday(), 7)
        self.assertEqual(timestamp.month, 7)
        self.assertEqual(timestamp.day, 3)
        self.assertEqual(timestamp.year, 2016)

    def test_fall_weekday(self):
        timestamp = utils.get_period_timestamp('weekday', 'fall', 2016)
        self.assertEqual(timestamp.isoweekday(), 1)
        self.assertEqual(timestamp.month, 9)
        self.assertEqual(timestamp.day, 26)
        self.assertEqual(timestamp.year, 2016)

    def test_fall_saturday(self):
        timestamp = utils.get_period_timestamp('saturday', 'fall', 2016)
        self.assertEqual(timestamp.isoweekday(), 6)
        self.assertEqual(timestamp.month, 10)
        self.assertEqual(timestamp.day, 1)
        self.assertEqual(timestamp.year, 2016)

    def test_fall_sunday(self):
        timestamp = utils.get_period_timestamp('sunday', 'fall', 2016)
        self.assertEqual(timestamp.isoweekday(), 7)
        self.assertEqual(timestamp.month, 10)
        self.assertEqual(timestamp.day, 2)
        self.assertEqual(timestamp.year, 2016)

    def test_winter_weekday(self):
        timestamp = utils.get_period_timestamp('weekday', 'winter', 2016)
        self.assertEqual(timestamp.isoweekday(), 1)
        self.assertEqual(timestamp.month, 12)
        self.assertEqual(timestamp.day, 28)
        self.assertEqual(timestamp.year, 2015)

    def test_winter_saturday(self):
        timestamp = utils.get_period_timestamp('saturday', 'winter', 2016)
        self.assertEqual(timestamp.isoweekday(), 6)
        self.assertEqual(timestamp.month, 1)
        self.assertEqual(timestamp.day, 2)
        self.assertEqual(timestamp.year, 2016)

    def test_winter_sunday(self):
        timestamp = utils.get_period_timestamp('sunday', 'winter', 2016)
        self.assertEqual(timestamp.isoweekday(), 7)
        self.assertEqual(timestamp.month, 1)
        self.assertEqual(timestamp.day, 3)
        self.assertEqual(timestamp.year, 2016)


class CalibrateDayOfWeekTests(unittest.TestCase):
    """
    Tests utils.calibrate_day_of_week function.
    """

    def test_monday_before_first_of_month(self):
        # march 1, 2016 is a tuesday in US Central time
        timestamp = APP_TIMEZONE.localize(datetime(year=2016, month=3, day=1))
        self.assertEqual(2, timestamp.isoweekday())
        timestamp = utils.calibrate_day_of_week(timestamp, 'weekday').astimezone(APP_TIMEZONE)
        self.assertEqual(1, timestamp.isoweekday())
        self.assertEqual(timestamp.month, 2)
        self.assertEqual(timestamp.day, 29)
        self.assertEqual(timestamp.year, 2016)

    def test_saturday_after_first_of_month(self):
        # march 1, 2016 is a tuesday
        timestamp = APP_TIMEZONE.localize(datetime(year=2016, month=3, day=1))
        self.assertEqual(2, timestamp.isoweekday())
        timestamp = utils.calibrate_day_of_week(timestamp, 'saturday').astimezone(APP_TIMEZONE)
        self.assertEqual(6, timestamp.isoweekday())
        self.assertEqual(timestamp.month, 3)
        self.assertEqual(timestamp.day, 5)
        self.assertEqual(timestamp.year, 2016)

    def test_saturday_before_first_of_month(self):
        # november 1, 2015 is a sunday
        timestamp = APP_TIMEZONE.localize(datetime(year=2015, month=11, day=1))
        self.assertEqual(7, timestamp.isoweekday())
        timestamp = utils.calibrate_day_of_week(timestamp, 'saturday').astimezone(APP_TIMEZONE)
        self.assertEqual(6, timestamp.isoweekday())
        self.assertEqual(timestamp.month, 10)
        self.assertEqual(timestamp.day, 31)
        self.assertEqual(timestamp.year, 2015)

    def test_sunday_after_first_of_month(self):
        # march 1, 2016 is a tuesday
        timestamp = APP_TIMEZONE.localize(datetime(year=2016, month=3, day=1))
        self.assertEqual(2, timestamp.isoweekday())
        timestamp = utils.calibrate_day_of_week(timestamp, 'sunday').astimezone(APP_TIMEZONE)
        self.assertEqual(7, timestamp.isoweekday())
        self.assertEqual(timestamp.month, 3)
        self.assertEqual(timestamp.day, 6)
        self.assertEqual(timestamp.year, 2016)

    def test_sunday_before_first_of_month(self):
        # june 1, 2015 is a monday
        timestamp = APP_TIMEZONE.localize(datetime(year=2015, month=6, day=1))
        self.assertEqual(1, timestamp.isoweekday())
        timestamp = utils.calibrate_day_of_week(timestamp, 'sunday').astimezone(APP_TIMEZONE)
        self.assertEqual(7, timestamp.isoweekday())
        self.assertEqual(timestamp.month, 6)
        self.assertEqual(timestamp.day, 7)
        self.assertEqual(timestamp.year, 2015)
