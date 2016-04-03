"""
ETL module.
"""
import datetime
import json
import re
import xlrd
from xlrd.biffh import XLRDError
from sqlalchemy.orm.exc import NoResultFound
from .models import Route


def check_for_headers(cell, worksheet, row_counter, result):
    if cell.value == 'Route':
        result['numbers_available'] = True
        # check for route names
        candidate_name_header_cell = worksheet.cell(row_counter, 1)
        if candidate_name_header_cell.value == 'Route Name':
            result['names_available'] = True
        # check for route names
        candidate_name_header_cell = worksheet.cell(row_counter, 2)
        if candidate_name_header_cell.value == 'Route Type':
            result['types_available'] = True
        return True
    return False


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
    for row_index in range(minimum_search):
        try:
            cell = worksheet.cell(row_index, column_index)
            season, year = get_season_and_year(cell.value)
            day_of_week = extract_day_of_week(row_index, column_index, worksheet)
            if season and year and day_of_week:
                periods[str(column_index)] = {
                    'column': column_index,
                    'season': season.lower(),
                    'year': int(year),
                    'day_of_week': day_of_week.lower()
                }
                return True
        except IndexError:
            return False
    return False


def get_periods(worksheet, minimum_search=10):
    """
    Scans columns for period headers to create
    a helpful dict that matches season and year info to
    a worksheet column.

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
    route = session.query(Route).filter_by(route_number=route_number).one()
    try:
        current_instance = session.query(ridership_model).\
                        filter_by(route_id=route.id, season=period['season'],
                                  year=period['year'], day_of_week=period['day_of_week'],
                                  current=True).one()
        current_instance.current = False
    except NoResultFound:
        pass


def handle_ridership_cell(route_number, period, ridership_cell,
                          ridership_model, session, time_zone):
    # check for a number cell
    if ridership_cell.ctype == 2:
        deactivate_current_period(route_number, period, ridership_model, session)
        route = session.query(Route).filter_by(route_number=route_number).one()
        new_ridership = ridership_model(route_id=route.id,
                                        current=True,
                                        day_of_week=period['day_of_week'],
                                        season=period['season'],
                                        year=period['year'],
                                        ridership=ridership_cell.value,
                                        created_on=datetime.datetime.now(tz=time_zone))
        session.add(new_ridership)


def parse_worksheet_ridership(worksheet, periods, ridership_model, session, time_zone):
    route_number_cells = worksheet.col(0)
    row_counter = 0
    # let's iterate down the rows
    for cell in route_number_cells:
        if cell.ctype == 2:
            route_number = int(cell.value)
            session.query(ridership_model)
            # now let's check each column with a ridership period to get its metric
            for column, period_data in periods.items():
                try:
                    ridership_cell = worksheet.cell(row_counter, int(column))
                    handle_ridership_cell(route_number, period_data, ridership_cell,
                                          ridership_model, session, time_zone)
                except XLRDError:
                    pass
        row_counter += 1


def update_ridership(file_location, worksheet_names, ridership_model, session):
    for worksheet_name in worksheet_names:
        excel_book = xlrd.open_workbook(filename=file_location)
        worksheet = excel_book.sheet_by_name(worksheet_name)
        periods = get_periods(worksheet_name)
        parse_worksheet_ridership(worksheet, periods, ridership_model, session)


def get_route_info(file_location, worksheet_name):
    """
    This function begins by iterates through the rows in a worksheet,
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
    result = {
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
        if not check_for_headers(cell, worksheet, row_counter, result) \
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
            if result['names_available'] and candidate_route_name_cell.ctype == 1:
                route_info['route_name'] = candidate_route_name_cell.value.upper()
            # try to get service type
            candidate_service_type_cell = worksheet.cell(row_counter, 2)
            # check if cell is text
            if result['types_available'] and candidate_service_type_cell.ctype == 1:
                route_info['service_type'] = candidate_service_type_cell.value.upper()
            result['routes'].append(route_info)
        row_counter += 1
    return result


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
    merged_data = {}
    for result in results:
        for route_info in result['routes']:
            merged_data[route_info['route_number']] = route_info
    return merged_data


def store_route(session, route_number, route_info):
    """
    Creates or updates a route from passed information.

    The route name and service type are converted to all-caps strings.

    Args:
        session: An SQLAlchemy session.
        route_number (str): The digit-only label for a route number.
        route_info (dict): Contains route name and service type data.
    """
    try:
        route = session.query(Route).filter_by(route_number=int(route_number)).one()
        route.route_name = route_info['route_name']
        route.service_type = route_info['service_type']
    except NoResultFound:
        new_route = Route(route_number=int(route_number),
                          route_name=route_info['route_name'].upper(),
                          service_type=route_info['service_type'].upper())
        session.add(new_route)


def update_route_info(file_location, session, worksheets):
    """
    Saves latest route model information into database.

    Args:
        file_location (str): Location of Excel file with data.
        session: SQLAlchemy database session.
        worksheets (list): The string names of the worksheets to be searched for route info.
    """
    results = list()
    for worksheet in worksheets:
        route_info = get_route_info(file_location, worksheet)
        results.append(route_info)
    merged_data = merge_route_data(results)
    for route_number, route_info in merged_data.items():
        store_route(session, route_number, route_info)
    session.commit()


