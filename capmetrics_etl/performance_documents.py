from collections import OrderedDict
import datetime
import json
from sqlalchemy.orm.exc import NoResultFound
import pytz
from . import models

TIMEZONE_NAME = 'America/Chicago'
APP_TIMEZONE = pytz.timezone(TIMEZONE_NAME)


def transform_ridership_collection(riderships, type_name, route_id, included):
    resource_identifiers = []
    for ridership in riderships:
        identification = [('id', str(ridership.id)), ('type', type_name)]
        resource_identifiers.append(OrderedDict(identification))
        zulu_created = ridership.created_on.isoformat().replace('+00:00', '')
        zulu_created = '{0}Z'.format(zulu_created)
        zulu_measurement = ridership.measurement_timestamp.isoformat().replace('+00:00', '')
        zulu_measurement = '{0}Z'.format(zulu_measurement)
        attributes = {
            'created-on': zulu_created,
            'is-current': ridership.is_current,
            'day-of-week': ridership.day_of_week,
            'season': ridership.season,
            'calendar-year': ridership.calendar_year,
            'ridership': ridership.ridership,
            'measurement-timestamp': zulu_measurement
        }
        relationships = {
            'route': {
                'data': {
                    'id': str(route_id),
                    'type': 'routes'
                }
            }
        }
        resource_object = OrderedDict(identification)
        resource_object['attributes'] = attributes
        resource_object['relationships'] = relationships
        included.append(resource_object)
    return resource_identifiers


def build_route_document(route):
    included = []
    daily_riderships = route.daily_ridership
    service_hour_riderships = route.service_hour_ridership
    daily_ridership_identifiers = transform_ridership_collection(daily_riderships,
                                                                 'daily-riderships',
                                                                 route.id,
                                                                 included)
    service_hour_ridership_identifiers = transform_ridership_collection(service_hour_riderships,
                                                                        'service-hour-riderships',
                                                                        route.id,
                                                                        included)
    relationships = {
        'daily-riderships': {'data': daily_ridership_identifiers},
        'service-hour-riderships': {'data': service_hour_ridership_identifiers}
    }
    identification = [('id', str(route.id)), ('type', 'routes')]
    primary_data = OrderedDict(identification)
    primary_data['attributes'] = {
        'route-number': route.route_number,
        'route-name': route.route_name,
        'service-type': route.service_type,
        'is-high-ridership': route.is_high_ridership
    }
    primary_data['relationships'] = relationships
    return json.dumps({'data': primary_data, 'included': included})


def build_system_trends_document(system_trends):
    primary_data = []
    for system_trend in system_trends:
        zulu_timestamp = system_trend.updated_on.isoformat().replace('+00:00', '')
        zulu_timestamp = '{0}Z'.format(zulu_timestamp)
        attributes = {
            # JavaScript expects a 'Z' to represent Zulu Time (i.e. UTC timezone)
            # Python's isoformat function does not append the 'Z' to UTC timezone datetime
            # objects.
            'updated-on': zulu_timestamp,
            'trend': system_trend.trend,
            'service-type': system_trend.service_type
        }
        resource_object = OrderedDict(
            [
                ('id', str(system_trend.id)),
                ('type', 'system-trends'),
                ('attributes', attributes)
            ]
        )
        primary_data.append(resource_object)
    return json.dumps({'data': primary_data})


def update_route_documents(session):
    routes = session.query(models.Route).all()
    for route in routes:
        name = 'route-{0}'.format(route.route_number)
        update_timestamp = datetime.datetime.now(tz=pytz.utc)
        document = build_route_document(route)
        try:
            performance_document = session.query(models.PerformanceDocument)\
                                      .filter_by(name=name).one()
            performance_document.document = document
        except NoResultFound:
            route_doc = models.PerformanceDocument(name=name,
                                                   document=document,
                                                   updated_on=update_timestamp)
            session.add(route_doc)
    session.commit()


def update_system_trends_document(session):
    system_trends = session.query(models.SystemTrend).all()
    document = build_system_trends_document(system_trends)
    update_timestamp = datetime.datetime.now(tz=pytz.utc)
    try:
        system_trends_doc = session.query(models.PerformanceDocument) \
            .filter_by(name='system-trends').one()
        system_trends_doc.document = document
        system_trends_doc.updated_on = update_timestamp
    except NoResultFound:
        system_trends_doc = models.PerformanceDocument(name='system-trends',
                                                       document=document,
                                                       updated_on=update_timestamp)
        session.add(system_trends_doc)
    session.commit()


class RouteCompendiumEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, models.DailyRidership):
            return {
                'id': str(obj.id),
                'createdOn': obj.created_on.isoformat(),
                'isCurrent': obj.is_current,
                'dayOfWeek': obj.day_of_week,
                'season': obj.season,
                'calendarYear': obj.calendar_year,
                'ridership': int(obj.ridership),
                'routeId': obj.route_id,
                'measurementTimestamp': obj.measurement_timestamp.isoformat()
            }
        return json.JSONEncoder.default(self, obj)


def sort_compendium_riderships(compendiums):
    for compendium in compendiums:
        compendium['riderships'].sort(key=lambda r: r.measurement_timestamp)


def update_top_routes(session):
    high_ridership_routes = session.query(models.Route) \
        .filter_by(is_high_ridership=True) \
        .all()
    top_routes = []
    for route in high_ridership_routes:
        active_ridership = session.query(models.DailyRidership).filter_by(route_id=route.id, is_current=True)
        for ridership in active_ridership:
            compendium = next((c for c in top_routes if c['routeNumber'] == str(route.route_number)), None)
            if compendium:
                compendium['riderships'].append(ridership)
            else:
                selector = 'top-route-viz-{0}'.format(str(route.route_number))
                compendium = {
                    'routeNumber': str(route.route_number),
                    'routeName': route.route_name,
                    'selector': selector,
                    'riderships': [ridership]
                }
                top_routes.append(compendium)
    sort_compendium_riderships(top_routes)
    document = json.dumps(top_routes, cls=RouteCompendiumEncoder)
    update_timestamp = datetime.datetime.now(tz=pytz.utc)
    try:
        top_routes_doc = session.query(models.PerformanceDocument) \
            .filter_by(name='top-routes').one()
        top_routes_doc.document = document
        top_routes_doc.updated_on = update_timestamp
    except NoResultFound:
        top_routes_doc = models.PerformanceDocument(name='top-routes',
                                                    document=document,
                                                    updated_on=update_timestamp)
        session.add(top_routes_doc)
    session.commit()


def update(session):
    update_system_trends_document(session)
    update_route_documents(session)
    update_top_routes(session)
