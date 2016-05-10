"""
Data quality assurance functions.
"""
import xlrd
from xlrd.biffh import XLRDError
from capmetrics_etl import etl


def check_worksheet_completeness(file_location, worksheet_names):
    """
    Checks for a list of worksheet names for data extraction. For example::

            'Ridership by Route Weekday',
            'Ridership by Route Saturday',
            'Ridership by Route Sunday',
            'Riders per Hour Weekday',
            'Riders Hour Saturday',
            'Riders per Hour Sunday'

    Args:
        file_location: The location of the target Excel file.
        worksheet_names (list): A list of string representing Excel worksheet names.

    Returns:
        tuple: A boolean indicating whether the required worksheets were found and
        a list of the worksheet names that were not found (if any).

    """
    excel_book = xlrd.open_workbook(filename=file_location)
    misses = list()
    for name in worksheet_names:
        try:
            excel_book.sheet_by_name(name)
        except XLRDError:
            misses.append(name)
    if len(misses):
        return False, misses
    return True, misses


def check_route_info(file_location, worksheet_name):
    """
    Determines if the passed worksheet has route data.

    Args:
        file_location (str): The Excel file to examine.
        worksheet_name (str): A worksheet name string.

    Returns:
        bool: ``True`` if all worksheet has route data. ``False`` otherwise.
    """
    route_info = etl.get_route_info(file_location, worksheet_name)
    return True if len(route_info['routes']) > 0 else False


def check_route_presence(file_location, worksheet_names):
    """
    Determines if the passed worksheets have route data.

    Args:
        file_location (str): The Excel file to examine.
        worksheet_names (list): A list of worksheet name strings.

    Returns:
        bool: ``True`` if all worksheets have route data. ``False`` otherwise.
    """
    for worksheet_name in worksheet_names:
        has_route_info = check_route_info(file_location, worksheet_name)
        if not has_route_info:
            return False
    return True


def has_ridership_data_column(worksheet, floor=10):
    """
    Determines if passed worksheet contains a ridership data point.

    Args:
        worksheet: An Excel worksheet.
        floor (int): The minimum number of columns and rows to search
            for the initial ridership data point.

    Returns:
        bool: ``True`` if worksheet has ridership data. ``False`` otherwise.
    """
    try:
        for column_index in range(floor):
            try:
                for row_index in range(floor):
                    cell = worksheet.cell(row_index, column_index)
                    season, year = etl.get_season_and_year(cell.value)
                    if season and year:
                        return True
            except IndexError:
                continue
    except IndexError:
        pass
    return False


def check_for_ridership_columns(file_location, worksheet_names):
    """
    Determines if all passed worksheets contain ridership data.

    Args:
        file_location (str): The Excel file to examine.
        worksheet_names (list): A list of worksheet name strings.

    Returns:
        bool: ``True`` if all worksheets have ridership data. ``False`` otherwise.
    """
    excel_book = xlrd.open_workbook(filename=file_location)
    for worksheet_name in worksheet_names:
        has_data = has_ridership_data_column(excel_book.sheet_by_name(worksheet_name))
        if not has_data:
            return False
    return True


def check_quality(file_location, worksheet_names):
    """
    Runs 'sanity check' on data quality.  Specifically:

    1. Worksheet completeness - Check for the presence of all six of the worksheets from which data is extracted.

    2. Route rows present - Check for at least one data point of route number and route name column
       in the 6 ridership worksheets.

    3. Ridership columns present - Check for at least one ridership data column in all 6 ridership data worksheets.

    Args:
        file_location (str): The data file location.
        worksheet_names (list): A list of string names with the worksheets that will be
            checked for data quality.

    Returns:
        bool: ``True`` if the worksheets pass the data quality check. ``False`` otherwise.
    """
    if not check_worksheet_completeness(file_location, worksheet_names)[0]:
        print('Incomplete worksheets')
        return False
    if not check_route_presence(file_location, worksheet_names):
        print('Missing routes')
        return False
    if not check_for_ridership_columns(file_location, worksheet_names):
        print('Missing ridership')
        return False
    return True
