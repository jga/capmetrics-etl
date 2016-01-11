import json
import copy
import math
import sqlalchemy
import xlrd


class ServiceName(object):
    def __init__(self, name=None):
        self.name = name


class ServiceNumber(object):
    def __init__(self, number=None):
        self.number = number


class ServiceInfo:
    def __init__(self, name=None, number=None):
        self.name = name
        self.number = number

    def full_name(self):
        empty = True
        full_name = ''
        if self.number:
            full_name.join((' ', str(self.number)),)
            empty = False
        if self.name:
            full_name.join((' ', self.name,))
            empty = False
        if empty:
            return ''.join('Transit Service Info')
        else:
            return ''.join(('Transit Service Info for ', full_name,))

    #def __repr__(self):
    #    return self.full_name()


class ChartistBarVisualization(object):

    def __init__(self, title=None, labels=None, series=None):
        self.title = title
        self.labels = labels
        self.series = series


class Ridership(object):
    """
    A datetime is necessary for ordering and sorting in visualizations. If the
    actual temporal object is not a datetime, then by convention we provide the
    time of the temporal object starts at.

    The temporal label provides support for alternative naming of a datetime.
    For example, it might be useful to indicate that a datetime's result is
    are actually representative of a season, or a month, or a
    """
    def __init__(self, dt, service_number=None, service_name=None, result=None, temporal_label=None, id=None):
        self.id = id
        self.datetime = dt
        self.service_number = service_number
        self.service_name = service_name
        self.result = result
        self.temporal_label = temporal_label

    # def __repr__(self):
    #     if self.temporal_label:
    #         return self.temporal_label
    #     return ''.join((self.datetime, ' ', self.result,))


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



def update_routes():
    pass


def update_modes():
    pass
