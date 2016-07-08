import datetime
import pytz

TIMEZONE_NAME = 'America/Chicago'
APP_TIMEZONE = pytz.timezone(TIMEZONE_NAME)


def calibrate_day_of_week(timestamp, day_of_week):
    """
    Adjusts the passed datetime to reflect the desired day of week. The
    calibration chooses the requested day of week that is present in the
    **week of the original datetime timestamp**. In the case of a Monday
    timestamp being passed with a 'sunday' day of week argument,
    that means that the returned, calibrated Sunday updated onto the
    timestamp will not be the immediately previous day but instead the
    end of the week.

    Args:
        timestamp: A Python ``datetime`` reflecting when a ridership data point occured.
        day_of_week (str): 'weekday', 'saturday', or 'sunday' expected.  A Monday is
            used as the substitute for 'weekday'.

    Returns:
        The Python datetime timestamp with date that conforms to required day of week in
        CMTA's Central Time ('America/Chicago'). The returned timestamp for storage is in
        UTC timezone. Client applications must localize the UTC back into Central.
    """
    target_day = 1
    if day_of_week == 'saturday':
        target_day = 6
    elif day_of_week == 'sunday':
        target_day = 7
    # immediately return if already at that day
    current_day = timestamp.isoweekday()
    if current_day == target_day:
        return timestamp.astimezone(pytz.utc)
    difference = target_day - current_day
    timestamp = timestamp + datetime.timedelta(days=difference)
    return timestamp.astimezone(pytz.utc)


def get_period_timestamp(day_of_week, season, calendar_year):
    """
    Selects a Python datetime timestamp to represent a 'season' as
    a specific day of the year. The function selects the first day of
    week matched the passed argument in the first month of the season
    matching the passed in season.

    For 'winter', the selected month is January. For 'spring', the selected
    month is April. For 'summer', the selected month is July. For fall,
    the selected month is October. While not perfectly matching the actual
    seasons, it matches a quarterly schedule that is intuitive.

    Args:
        day_of_week (str): 'weekday', 'saturday', and 'sunday' are expected.
        season (str): 'winter', 'spring', 'summer', 'fall' are expected.
        calendar_year (int): The calendar year.

    Returns:
        A Python datetime object that represents that 'season'. It's timezone is UTC.
    """
    month = 1
    if season == 'spring':
        month = 4
    elif season == 'summer':
        month = 7
    elif season == 'fall':
        month = 10
    # timezone aware timestamp for a central 'America/Chicago' timezone
    timestamp = APP_TIMEZONE.localize(datetime.datetime(year=calendar_year,
                                                        month=month,
                                                        day=1))
    # returns a UTC (not 'America/Chicago') timezone datetime - the return data will
    # be persisted, and that's the expected practice
    return calibrate_day_of_week(timestamp, day_of_week.lower())
