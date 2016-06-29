from collections import OrderedDict
import datetime
import json
from sqlalchemy.orm.exc import NoResultFound
import pytz
from . import models

TIMEZONE_NAME = 'America/Chicago'
APP_TIMEZONE = pytz.timezone(TIMEZONE_NAME)


class PerformanceDocumentEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


def transform_ridership_collection(riderships, type_name, route_id, included):
    resource_identifiers = []
    for ridership in riderships:
        identification = [('id', ridership.id), ('type', type_name)]
        resource_identifiers.append(OrderedDict(identification))
        attributes = {
            'created-on': ridership.created_on,
            'is-current': ridership.is_current,
            'day-of-week': ridership.day_of_week,
            'season': ridership.season,
            'calendar-year': ridership.calendar_year,
            'ridership': ridership.ridership,
            'measurement-timestamp': ridership.measurement_timestamp
        }
        relationships = {
            'route': {
                'data': {
                    'id': route_id,
                    'type': 'route'
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
    identification = [('id', route.id), ('type', 'routes')]
    document = OrderedDict(identification)
    document['attributes'] = {
        'route-number': route.route_number,
        'route-name': route.route_name,
        'service-type': route.service_type,
        'is-high-ridership': route.is_high_ridership
    }
    document['relationships'] = relationships
    document['included'] = included
    return document


def build_system_trends_document(system_trends):
    primary_data = []
    for system_trend in system_trends:
        attributes = {
            # JavaScript expects a 'Z' to represent Zulu Time (i.e. UTC timezone)
            # Python's isoformat function does not append the 'Z' to UTC timezone datetime
            # objects.
            'updated-on': '{0}Z'.format(system_trend.updated_on.isoformat()),
            'trend': system_trend.trend,
            'service-type': system_trend.service_type
        }
        resource_object = OrderedDict(
            [
                ('id', system_trend.id),
                ('type', 'system-trends'),
                ('attributes', attributes)
            ]
        )
        primary_data.append(resource_object)
    return json.dumps({'data': primary_data})


def update_system_trends_document(session):
    system_trends = session.query(models.SystemTrend).all()
    document = build_system_trends_document(system_trends)
    update_timestamp = datetime.datetime.now(tz=pytz.utc)
    try:
        system_trends_doc = session.query(models.PerformanceDocument)\
                               .filter_by(name='system-trends').one()
        system_trends_doc.document = document
        system_trends_doc.updated_on = update_timestamp
    except NoResultFound:
        system_trends_doc = models.PerformanceDocument(name='system-trends',
                                                       document=document,
                                                       updated_on=update_timestamp)
        session.add(system_trends_doc)
    session.commit()


def update_route_documents(session):
    routes = session.query(models.Route).all()
    for route in routes:
        document = build_route_document(route)
        name = 'route-{0}'.format(route.id)
        performance_document = session.query(models.PerformanceDocument)\
                                      .filter_by(name=name).one()
        performance_document.document = document
    session.commit()


def update(session):
    update_system_trends_document(session)
    update_route_documents(session)