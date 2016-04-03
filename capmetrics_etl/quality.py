"""
Data quality assurance functions.
"""
import xlrd
from xlrd.biffh import XLRDError
from capmetrics_etl import etl


def check_worksheets(file_location, worksheet_names):
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
    route_info = etl.get_route_info(file_location, worksheet_name)
    return True if len(route_info['routes']) >  0 else False


def has_ridership_data_column(worksheet, floor=10):
    for column_index in range(floor):
        for row_index in range(floor):
            cell = worksheet.cell(row_index, column_index)
            season, year = etl.get_season_and_year(cell.value)
            if season and year:
                return True
    return False


def check_for_ridership_columns(file_location):
    excel_book = xlrd.open_workbook(filename=file_location)
    worksheet_names = [
        'Ridership by Route Weekday',
        'Ridership by Route Saturday',
        'Ridership by Route Sunday',
        'Riders per Hour Weekday',
        'Riders Hour Saturday',
        'Riders per Hour Sunday'
    ]
    for worksheet_name in worksheet_names:
        has_data = has_ridership_data_column(excel_book.sheet_by_name(worksheet_name))
        if not has_data:
            return False
    return True
