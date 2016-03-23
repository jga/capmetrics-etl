"""
Data quality assurance functions.
"""
import xlrd
from xlrd.biffh import XLRDError


def check_worksheets(file_location):
    """
    Checks for a list of worksheet names for data extraction. The six names are::

            'Ridership by Route Weekday',
            'Ridership by Route Saturday',
            'Ridership by Route Sunday',
            'Riders per Hour Weekday',
            'Riders Hour Saturday',
            'Riders per Hour Sunday'

    Args:
        file_location: The location of the target Excel file.

    Returns:
        tuple: A boolean indicating whether the required worksheets were found and
        a list of the worksheet names that were not found (if any).

    """
    excel_book = xlrd.open_workbook(filename=file_location)
    required_names = [
        'Ridership by Route Weekday',
        'Ridership by Route Saturday',
        'Ridership by Route Sunday',
        'Riders per Hour Weekday',
        'Riders Hour Saturday',
        'Riders per Hour Sunday'
    ]
    misses = list()
    for name in required_names:
        try:
            excel_book.sheet_by_name(name)
        except XLRDError:
            misses.append(name)
    if len(misses):
        return False, misses
    return True, misses


def check_route_info(file_location, worksheet_name):
    result = {
        'numbers_available': False,
        'names_available': False,
        'route_numbers': [],
        'route_names': []
    }
    excel_book = xlrd.open_workbook(filename=file_location)
    worksheet = excel_book.sheet_by_name(worksheet_name)
    first_column_cells = worksheet.col(0)
    row_counter = 0
    for cell in first_column_cells:
        if cell.value == 'Route':
            result['numbers_available'] = True
            # check for route names
            candidate_name_header_cell = worksheet.cell(row_counter, 1)
            if candidate_name_header_cell.value == 'Route Name':
                result['names_available'] = True
        # check if cell is a number
        elif cell.ctype == 2:
            result['route_numbers'].append(str(int(cell.value)))
            # try to get route name
            candidate_route_name_cell = worksheet.cell(row_counter, 1)
            # check if cell is text
            if result['names_available'] and candidate_route_name_cell.ctype == 1:
                result['route_names'].append(candidate_route_name_cell.value)
        row_counter += 1
    return result


