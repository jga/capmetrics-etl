"""
Extract-Transform-Load functions.
"""
from collections import OrderedDict
import datetime
import json
import os
import pytz
import re
from dateutil.parser import parse
from sqlalchemy import asc, desc, func
from sqlalchemy.orm.exc import NoResultFound
import xlrd
from xlrd.biffh import XLRDError
from . import models
from . import performance_documents as perfdocs

TIMEZONE_NAME = 'America/Chicago'
APP_TIMEZONE = pytz.timezone(TIMEZONE_NAME)


def check_for_headers(cell, worksheet, row_counter, worksheet_routes):
    if cell.value == 'Route':
        worksheet_routes['numbers_available'] = True
        # check for route names
        candidate_name_header_cell = worksheet.cell(row_counter, 1)
        if candidate_name_header_cell.value == 'Route Name':
            worksheet_routes['names_available'] = True
        # check for route names
        candidate_name_header_cell = worksheet.cell(row_counter, 2)
        if candidate_name_header_cell.value == 'Route Type':
            worksheet_routes['types_available'] = True
        return True
    return False


def create_tables(engine):
    models.Base.metadata.create_all(engine)


def extract_day_of_week(period_row, period_column, worksheet):
    """
    Extracts the day of week for a ridership data column be searching
    for 'weekday' or 'saturday' or 'sunday' strings under a period header.

    Args:
        period_row: Row for the period header. The day of week should be under it.
        period_column: Column for the period header.
        worksheet: The worksheet where the search occurs.

    Returns:
        A string with the day of week if one present; ``None`` otherwise.
    """
    day_of_week = None
    day_cell = worksheet.cell((period_row + 1), period_column)
    if day_cell.ctype == 1:
        day = day_cell.value.lower()
        regex_pattern = re.compile(r'(weekday|saturday|sunday)')
        result = regex_pattern.match(day)
        if result:
            return result.group(1)
    return day_of_week


def get_season_and_year(period):
    if isinstance(period, str):
        period = period.lower()
        regex_pattern = re.compile(r'(summer|fall|winter|spring) +(\d\d\d\d)')
        result = regex_pattern.match(period)
        if result:
            return result.group(1), result.group(2)
    return None, None


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
        return timestamp
    difference = target_day - current_day
    timestamp = timestamp + datetime.timedelta(days=difference)
    return timestamp.astimezone(pytz.utc)


def get_latest_measurement_timestamp(session):
    """
    Queries :class:`~.models.DailyRidership` models to find the
    latest (most recent) measurement timestamp (which is a datetime object).

    Args:
        session: SQLAlchemy session.

    Returns:
        datetime.datetime
    """
    latest = session.query(models.DailyRidership)\
                    .filter_by(day_of_week='weekday')\
                    .order_by(desc(models.DailyRidership.measurement_timestamp))\
                    .first()
    return latest.measurement_timestamp


def get_high_ridership_routes(session, timestamp, size=10):
    """
    Determines the ``Routes`` with highest ``DailyRidership`` counts
    of ridership.

    Args:
        session: SQLAlchemy session
        timestamp: A datetime object serving as the timestamp that will filter ridership data.
        size: The number of routes to return. The default is 10, so the 'top ten' routes are returned.

    Returns:
        list
    """
    top_riderships = session.query(models.DailyRidership)\
           .filter_by(day_of_week='weekday', measurement_timestamp=timestamp)\
           .order_by(desc(models.DailyRidership.ridership))[0:size]
    return [top.route.route_number for top in top_riderships]


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
    # returns a UTC (not 'America/Chicago') timezone as it will be persisted
    return calibrate_day_of_week(timestamp, day_of_week.lower())


def find_period(worksheet, periods, column_index, minimum_search=10):
    """
    Searches for a performance period in a column and places
    the period information into a passed-in dictionary.

    Args:
        worksheet: The spreadsheet where the column search occurs.
        periods (dict): The dict holding period data for the worksheet's columns.
        column_index (int): The column searched.
        minimum_search (int): The number of rows without a match searched before quitting.

    Returns:
        boolean: ``True`` if a period is found.
    """
    try:
        for row_index in range(minimum_search):
            cell = worksheet.cell(row_index, column_index)
            season, year = get_season_and_year(cell.value)
            day_of_week = extract_day_of_week(row_index, column_index, worksheet)
            if season and year and day_of_week:
                # The timestamp is in UTC timezone
                period_timestamp = get_period_timestamp(day_of_week, season.lower(), int(year))
                periods[str(column_index)] = {
                    'column': column_index,
                    'season': season.lower(),
                    'year': int(year),
                    'timestamp': period_timestamp,
                    'day_of_week': day_of_week.lower()
                }
                return True
    except IndexError:
        pass
    return False


def get_periods(worksheet, minimum_search=10):
    """
    Scans columns for period headers to create a helpful dict that matches season and year
    info to a worksheet column.

    Period info dictionaries include ``column``, ``season``, ``year``,
    ``day_of_week`` keys.

    Args:
        worksheet: Excel worksheet searched.
        minimum_search: The number of columns without a period
            that are searched before quitting.

    Returns:
        dict: Period info dicts keyed to column.
    """
    periods = {}
    not_periods = 0
    column_index = 0
    while not_periods < minimum_search:
        found = find_period(worksheet, periods, column_index, minimum_search)
        if not found:
            not_periods += 1
        column_index += 1
    return periods


def deactivate_current_period(route_number, period, ridership_model, session):
    """
    "Deactivates" a ridership metric model by setting its ``is_current`` property
    to ``False``.

    Args:
        route_number (int): A service route's identifying number.
        period (dict): The covered season and year.
        ridership_model: SQLAlchemy model *class* that persists a period's ridership metric.
        session: SQLAlcemeny session instance.

    Returns:
        bool: ``True`` if an existing instance had
            its ``is_current`` property changed. ``False`` otherwise.
    """
    route = session.query(models.Route).filter_by(route_number=route_number).one()
    try:
        current_instance = session.query(ridership_model).\
                            filter_by(route_id=route.id,
                                      season=period['season'],
                                      calendar_year=period['year'],
                                      day_of_week=period['day_of_week'],
                                      is_current=True).one()
        current_instance.is_current = False
        return True
    except NoResultFound:
        return False


def handle_ridership_cell(route_number, period, ridership_cell,
                          ridership_model, session, report=None):
    """
    Extracts ridership metric and deactivates previous versions
    of a performance metric for a specific period.

    Args:
        route_number (int): A transit service route number.
        period (dict): Includes ``day_of_week``, ``season``, ``year``, ``timestamp`` keys.
        ridership_cell: The spreadsheet cell being handled.
        ridership_model: The SQLAlchemy model *class* that will be persisted.
        session: The SQLAlchemy session.
        report: An optional :class:`~.models.ETLReport` instance. Default value is ``None``

    Returns:
        The ETLReport instance if passed into function; ``None`` otherwise.
    """
    # check for a number cell
    if ridership_cell.ctype == 2:
        deactivation = deactivate_current_period(route_number, period,
                                                 ridership_model, session)
        if report and deactivation:
            report.updates += 1
        route = session.query(models.Route).filter_by(route_number=route_number).one()
        # This is now the current ridership data for the period
        new_ridership = ridership_model(route_id=route.id,
                                        is_current=True,
                                        day_of_week=period['day_of_week'],
                                        season=period['season'],
                                        calendar_year=period['year'],
                                        measurement_timestamp=period['timestamp'],
                                        ridership=ridership_cell.value,
                                        created_on=datetime.datetime.now(tz=pytz.utc))
        session.add(new_ridership)
        if report:
            report.creates += 1
    return report


def parse_worksheet_ridership(worksheet, periods, ridership_model,
                              session, report=None):
    """

    Parses an Excel worksheet by iterating down rows (routes) and
    columns (ridership by period) to create/update ridership data.

    For each row, the function examines the metrics in each
    column by iterating the ``periods`` dictionary passed into the
    function through the columns that make up the keys, with
    additional handling logic finding the data for the row's route within
    each column.

    Args:
        worksheet: The Excel worksheet to be parsed.
        periods (dict): Keyed to the column, with period data as the value.
        ridership_model: A ridership metric model such as ``DailyRidership``.
        session: SQLAlchemy database session.
        report: An optional :class:`~.models.ETLReport`.
    """
    route_number_cells = worksheet.col(0)
    row_counter = 0
    # let's iterate down the rows
    for cell in route_number_cells:
        if cell.ctype == 2:
            route_number = int(cell.value)
            # now let's process each column with that has a ridership period header
            for column, period_data in periods.items():
                try:
                    ridership_cell = worksheet.cell(row_counter, int(column))
                    handle_ridership_cell(route_number, period_data, ridership_cell,
                                          ridership_model, session, report)
                except XLRDError:
                    pass
        row_counter += 1


def update_ridership(file_location, worksheet_names, ridership_model, session):
    """

    Args:
        file_location (str): The location of a data store Excel file.
        worksheet_names (list): A list of strings with Excel file worksheet names.
        ridership_model: A ridership model, such as :class:`~.models.DailyRidership` model.
        session: A SQLAlchemy session.
    Returns:
        An :class:`~.models.ETLReport`.
    """
    etl_report = models.ETLReport(
        created_on=datetime.datetime.now(tz=pytz.utc),
        updates=0,
        creates=0,
        total_models=None
    )
    for worksheet_name in worksheet_names:
        excel_book = xlrd.open_workbook(filename=file_location)
        worksheet = excel_book.sheet_by_name(worksheet_name)
        periods = get_periods(worksheet)
        parse_worksheet_ridership(worksheet, periods, ridership_model,
                                  session, etl_report)
    session.commit()
    # avoids sub-querying performance hit on MySQL
    query = session.query(func.count(ridership_model.id)).group_by(ridership_model.id)
    etl_report.total_models = query.count()
    return etl_report


def get_route_info(file_location, worksheet_name):
    """
    This function begins by iterating through the rows in a worksheet,
    searching for route data or headers for route data.

    It returns a dict that indicates if it found properly formatted
    *header* data for route numbers, names, and service types, as well
    as the field data for each route's row.

    Args:
        file_location (str): Where the file with the data spreadsheets is located.
        worksheet_name (str): String name of the individual worksheet with cells
            that will be searched and processed.

    Returns:
        dict: The function returns a dict with ``numbers_available`` (boolean),
            ``names_available`` (boolean), ``routes`` (list)
    """
    worksheet_routes = {
        'numbers_available': False,
        'names_available': False,
        'types_available': False,
        'routes': [],
    }
    excel_book = xlrd.open_workbook(filename=file_location)
    worksheet = excel_book.sheet_by_name(worksheet_name)
    first_column_cells = worksheet.col(0)
    row_counter = 0
    for cell in first_column_cells:
        # if its not a header but is a number...
        if not check_for_headers(cell, worksheet, row_counter, worksheet_routes) \
                and cell.ctype == 2:
            # try to build a route info dict; first, check for a number
            route_number = str(int(cell.value))
            # start info dict with route number as safe default for route name
            route_info = {
                'route_number': route_number,
                'route_name': route_number,
                'service_type': ''
            }
            # try to get route name
            candidate_route_name_cell = worksheet.cell(row_counter, 1)
            # check if cell is text
            if worksheet_routes['names_available'] and candidate_route_name_cell.ctype == 1:
                route_info['route_name'] = candidate_route_name_cell.value.upper()
            # try to get service type
            candidate_service_type_cell = worksheet.cell(row_counter, 2)
            # check if cell is text
            if worksheet_routes['types_available'] and candidate_service_type_cell.ctype == 1:
                route_info['service_type'] = candidate_service_type_cell.value.upper()
            worksheet_routes['routes'].append(route_info)
        row_counter += 1
    return worksheet_routes


def merge_route_data(results):
    """
    Ensures that there is only one route info dict per route number. The
    approach is pretty simple - the last route info dict parsed is the
    final set value into the returned dict.

    Args:
        results (list): A list of dictionaries with route info keyed to ``routes``.

    Returns:
        dict: A dict of route info dict values keyed to route numbers.

    """
    merged_data = dict()
    for worksheet_routes in results:
        for route_info in worksheet_routes['routes']:
            merged_data[route_info['route_number']] = route_info
    return merged_data


def store_route(session, route_number, route_info, report=None):
    """
    Creates or updates a route from passed information.

    The route name and service type are converted to all-caps strings.

    Args:
        session: An SQLAlchemy session.
        route_number (str): The digit-only label for a route number.
        route_info (dict): Contains route name and service type data.
        report: Optional :class:`~.models.ETLReport` model for capturing ETL operations data.
    """
    try:
        route = session.query(models.Route).filter_by(route_number=int(route_number)).one()
        route.route_name = route_info['route_name'].upper()
        route.service_type = route_info['service_type'].upper()
        if report:
            report.updates += 1
    except NoResultFound:
        new_route = models.Route(route_number=int(route_number),
                                 route_name=route_info['route_name'].upper(),
                                 service_type=route_info['service_type'].upper())
        session.add(new_route)
        if report:
            report.creates += 1


def update_route_info(file_location, session, worksheets):
    """
    Saves latest route model information into database.

    Args:
        file_location (str): Location of Excel file with data.
        session: SQLAlchemy database session.
        worksheets (list): The string names of the worksheets to be searched for route info.
        timezone: A pytz-generated timezone info object.

    Returns:
        :class:`~.models.ETLReport`: A report with basic ETL job metrics
    """
    etl_report = models.ETLReport(
        created_on=datetime.datetime.now(tz=pytz.utc),
        updates=0,
        creates=0,
        total_models=None
    )
    results = list()
    for worksheet in worksheets:
        worksheet_routes = get_route_info(file_location, worksheet)
        results.append(worksheet_routes)
    merged_data = merge_route_data(results)
    for route_number, route_info in merged_data.items():
        store_route(session, route_number, route_info, etl_report)
    session.commit()
    # avoids sub-querying performance hit on MySQL
    query = session.query(func.count(models.Route.id)).group_by(models.Route.id)
    etl_report.total_models = query.count()
    return etl_report


def deactivate_previous_system_ridership_facts(session):
    """

    Selects all persisted ``SystemRidership`` models that are currently
    active and commits their ``is_active`` property to ``False``.

    Args:
        session: SQLAlchemy session.
    """
    facts = session.query(models.SystemRidership).filter_by(is_active=True).all()
    for fact in facts:
        fact.is_active = False
    session.commit()


def add_ridership_fact(system_facts, fact):
    """
    Fills the passed dict with a mapping of ISO 8601 timestamp keys
    to a ridership data dict. The ridership dict pairs service types
    with ridership facts, as well as a ``temporal`` key associated with a dict
    containing ``day_of_week``, ``season``, and ``calendar_year`` data. Example::

        {
            '2015-01-02T03:04:56-00:00': {
                'BUS': 10100,
                'RAIL': 220,
                'temporal': {
                    'day_of_week': 'weekday',
                    'season': 'winter',
                    'calendar_year': 2015
                }
            }
        }

    Args:
        system_facts (dict):
        fact (~`.models.DailyRidership`): A :class:`~.models.DailyRidership` instance.
    """
    timestamp = fact.measurement_timestamp.isoformat()
    if timestamp in system_facts:
        ridership_by_service = system_facts[timestamp]
        if fact.route.service_type in ridership_by_service:
            ridership_by_service[fact.route.service_type] = \
                ridership_by_service[fact.route.service_type] + fact.ridership
        else:
            ridership_by_service[fact.route.service_type] = fact.ridership
    else:
        system_facts[timestamp] = {
            fact.route.service_type: fact.ridership,
            'temporal': {
                'day_of_week': fact.day_of_week,
                'season': fact.season,
                'calendar_year': fact.calendar_year
            }
        }


def store_system_ridership(system_facts, session):
    """
    Creates and saves :class:`~.models.SystemRidership` models from
    passed data.

    Args:
        system_facts (dict): System ridership data.
        session: A SQLAlchemy session.
    """
    for measurement_timestamp, ridership_by_service in system_facts.items():
        for service_type, ridership in ridership_by_service.items():
            if service_type is not 'temporal':
                data = {
                    'calendar_year': ridership_by_service['temporal']['calendar_year'],
                    'created_on': datetime.datetime.now(pytz.utc),
                    'day_of_week': ridership_by_service['temporal']['day_of_week'],
                    'is_active': True,
                    'ridership': ridership,
                    'season': ridership_by_service['temporal']['season'],
                    'measurement_timestamp': parse(measurement_timestamp),
                    'service_type': service_type
                }
                system_ridership = models.SystemRidership(**data)
                session.add(system_ridership)
    session.commit()


def update_system_ridership(session):
    """
    Updates the persisted ``SystemRidership`` models; it 'deactivates' the
    existing models and then saves new models.

    Args:
        session: An SQLAlchemy session.
    """
    deactivate_previous_system_ridership_facts(session)
    ridership_facts = session.query(models.DailyRidership).filter_by(is_current=True).all()
    system_facts = dict()
    for fact in ridership_facts:
        add_ridership_fact(system_facts, fact)
    store_system_ridership(system_facts, session)


def to_service_facts(ridership_facts):
    """

    Dictionary key hierarchy for aggregation.

    A ``service_facts`` dict is keyed to a service type string. That
    is associated with a ``season_facts`` dict which has
    keys for the different ISO 8601 timestamps for the first day of
    a fact's season - regardless of the original day of week for the fact.
    Each seasonal key is matched with a dict that keys day types in the
    data with a single ridership number.

    Example::

        {
            'BUS': {
                '2015-01-02T00:00:00-00:00': {
                    'weekday': 10100,
                    'saturday': 2300,
                    'sunday': 400
                }
            }
        }

    Args:
        ridership_facts (list): A list of ridership models.
    """
    service_facts = dict()
    for fact in ridership_facts:
        # we use the 'weekday' timestamp for the aggregated data
        measurement_timestamp = get_period_timestamp('weekday', fact.season, fact.calendar_year).isoformat()
        if fact.service_type.upper() in service_facts:
            season_facts = service_facts[fact.service_type.upper()]
            if measurement_timestamp in season_facts:
                # add ridership for day of week
                by_day = season_facts[measurement_timestamp]
                by_day[fact.day_of_week.lower()] = fact.ridership
            else:
                # initialize fact's timestamp down to ridership by day of week
                by_day = {
                    fact.day_of_week.lower(): fact.ridership
                }
                season_facts[measurement_timestamp] = by_day
        # initialize from fact's service type down to ridership for day of week
        else:
            by_day = {
                fact.day_of_week.lower(): fact.ridership
            }
            season_facts = OrderedDict([(measurement_timestamp, by_day)])
            service_facts[fact.service_type.upper()] = season_facts
    return service_facts


def update_system_trends(session):
    """
    Updates :class:`~.models.SystemTrend` models based on existing system trend data
    for service types.

    Args:
        session: SQL Alchemy session.
    """
    ridership_facts = session.query(models.SystemRidership)\
                             .filter_by(is_active=True)\
                             .order_by(asc(models.SystemRidership.measurement_timestamp))\
                             .all()
    service_facts = to_service_facts(ridership_facts)
    # aggregate the day of week data for a service type's season into one total
    for service_type, timestamp_ridership_facts in service_facts.items():
        service_trend = []
        for timestamp, ridership_by_day in timestamp_ridership_facts.items():
            total = 0
            for day_of_week, ridership in ridership_by_day.items():
                if day_of_week.lower() == 'weekday':
                    total += (5 * ridership)
                else:
                    total += ridership
            service_trend.append([timestamp, total])
        # now, persist trend into models
        update_timestamp = datetime.datetime.now(tz=pytz.utc)
        try:
            system_trend = session.query(models.SystemTrend)\
                                  .filter_by(service_type=service_type)\
                                  .one()
            system_trend.trend = json.dumps(service_trend)
            system_trend.updated_on = update_timestamp
        except NoResultFound:
            system_trend = models.SystemTrend(service_type=service_type,
                                              trend=json.dumps(service_trend),
                                              updated_on=update_timestamp)

            session.add(system_trend)
    session.commit()


def update_high_ridership_routes(session, size=10):
    session.query(models.Route).update({'is_high_ridership': False},
                                       synchronize_session=False)
    latest = get_latest_measurement_timestamp(session)
    route_numbers = get_high_ridership_routes(session, latest, size)
    session.query(models.Route)\
           .filter(models.Route.route_number.in_(route_numbers))\
           .update({'is_high_ridership': True},
                   synchronize_session=False)
    session.commit()


def run_excel_etl(data_source_file, session, configuration):
    """
    Consumes an Excel file with CapMetro data and updates database tables
    with the file's data.

    Args:
        data_source_file (str): Location of the Excel file to be analyzed.
        session: SQLAlchemy session.
        configuration (dict): ETL configuration settings.
    """
    file_location = os.path.abspath(data_source_file)
    daily_worksheets = configuration['daily_ridership_worksheets']
    hourly_worksheets = configuration['hour_productivity_worksheets']
    print('Updating route info...')
    route_info_report = update_route_info(file_location,
                                          session,
                                          daily_worksheets)
    route_info_report.etl_type = 'route-info'
    session.add(route_info_report)
    print('Updating daily ridership...')
    daily_ridership_report = update_ridership(file_location,
                                              daily_worksheets,
                                              models.DailyRidership,
                                              session)
    daily_ridership_report.etl_type = 'daily-ridership'
    session.add(daily_ridership_report)
    print('Updating hourly ridership...')
    hourly_ridership_report = update_ridership(file_location,
                                               hourly_worksheets,
                                               models.ServiceHourRidership,
                                               session)
    hourly_ridership_report.etl_type = 'hourly-ridership'
    session.add(hourly_ridership_report)
    session.commit()
    print('Updating system ridership...')
    update_system_ridership(session)
    print('Updating system trends...')
    update_system_trends(session)
    print('Updating high ridership routes...')
    update_high_ridership_routes(session)
    print('Updating performance documents...')
    perfdocs.update(session)
    session.close()

