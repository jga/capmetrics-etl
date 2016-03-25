import copy
import json
import math
import re
import sqlalchemy
import xlrd


def get_season_and_year(period):
    if isinstance(period, str):
        period = period.lower()
        regex_pattern = re.compile(r'(summer|fall|winter|spring) +(\d\d\d\d)')
        result = regex_pattern.match(period)
        if result:
            return result.group(1), result.group(2)
    return None, None


def get_route_info(file_location, worksheet_name):
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


def extract(file_location):
    excel_book = xlrd.open_workbook(filename=file_location)
    excel_worksheet = excel_book.sheet_by_index()

class RidershipEncoder(json.JSONEncoder):
    def default(self, o):
        return {
            'id': o.id,
            'serviceName': o.service_name,
            'serviceNumber': o.service_number,
            'datetime': str(o.datetime),
            'result': o.result,
            'temporalLabel': o.temporal_label
        }


def process_row(sheet, current_row, row_map):
    info = ServiceInfo()
    single_route_ridership_data = []
    holder = []
    for idx, column_format in enumerate(row_map):
        # process the columns for the row
        value = sheet.row_values(current_row, idx, (idx + 1))
        if column_format['cls'] == 'Ridership':
            ridership = Ridership(column_format['dt'], temporal_label=column_format['temporal_label'])
            ridership.result = value[0]
            holder.append(ridership)
        if column_format['cls'] == 'ServiceNumber':
            info.number = value[0]
        if column_format['cls'] == 'ServiceName':
            info.name = value[0]
    for rd in holder:
        rd.service_number = info.number
        rd.service_name = info.name
        single_route_ridership_data.append(rd)
    return single_route_ridership_data


def handle_route_seasons(sheet, opt):
    all_routes_ridership_objects = []
    row_map = opt['row_map']
    current_row = opt['data_rows'][0]
    end_row = opt['data_rows'][1]
    if current_row <= end_row:
        while current_row <= end_row:
            # process a row of data, which is a sequence of ridership data points
            single_route_ridership_data = process_row(sheet, current_row, row_map)
            for ridership in single_route_ridership_data:
                ridership.id = (len(all_routes_ridership_objects) + 1)
                all_routes_ridership_objects.append(ridership)
            current_row += 1
        with open('/Users/jga/dev/capmetrics-app/public/ridership.json', 'w') as outfile:
            json.dump(all_routes_ridership_objects, outfile, cls=RidershipEncoder, indent=2)


def update_payload(payload, label, value):
    payload['labels'].append(label)
    if not value:
        value = 0
    else:
        value = math.floor(value)
    payload['series'].append(value)


def to_chart_group(sheet, current_row, row_map):
    chart_payload = {
        'labels': [],
        'series': []
    }
    chart_group = {
        'serviceNumber': None,
        'serviceName': None,
        'weekday': copy.deepcopy(chart_payload),
        'saturday': copy.deepcopy(chart_payload),
        'sunday': copy.deepcopy(chart_payload)
    }
    for idx, column_format in enumerate(row_map):
        # process the columns for the row
        value = sheet.row_values(current_row, idx, (idx + 1))
        if column_format['cls'] == 'Ridership':
            if column_format['series'] == 'weekday':
                update_payload(chart_group['weekday'], column_format['temporal_label'], value[0])
            elif column_format['series'] == 'saturday':
                update_payload(chart_group['saturday'], column_format['temporal_label'], value[0])
            elif column_format['series'] == 'sunday':
                update_payload(chart_group['sunday'], column_format['temporal_label'], value[0])
        if column_format['cls'] == 'ServiceNumber':
            chart_group['serviceNumber'] = math.floor(value[0])
        if column_format['cls'] == 'ServiceName':
            chart_group['serviceName'] = value[0]
    return chart_group


def to_json(file_location, options, handler):
    book = xlrd.open_workbook(filename=file_location)
    sheet = book.sheet_by_index(options['sheet_number'])
    return handler(sheet, options)


def write_index(data_source, path):
    json_data = to_json(data_source, OPTIONS, etl.to_route_charts)
    output = template.render(route_ids=[], viz_data=json_data)
    with open(path, "wb") as fh:
        fh.write(output)


def write():
    write_index('/Users/jga/Downloads/Ridership by Route by Markup-Item 2.xlsx', '/Users/jga/dev/cmx-dev/index.html')


def to_route_charts(sheet, opt):
    charts = []
    row_map = opt['row_map']
    current_row = opt['data_rows'][0]
    end_row = opt['data_rows'][1]
    if current_row <= end_row:
        while current_row <= end_row:
            # process a row of data, which is a sequence of ridership data points
            chart_group = to_chart_group(sheet, current_row, row_map)
            charts.append(chart_group)
            current_row += 1

        return json.dumps(charts)
        #with open('/Users/jga/dev/cmx-dev/ridership_visualization.json', 'w') as outfile:
            #json.dump(charts, outfile, indent=2)
            #json.dump(charts, outfile, cls=ChartistBarVisualizationEncoder, indent=2)
