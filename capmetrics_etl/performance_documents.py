from collections import OrderedDict
import datetime
import json
from sqlalchemy import asc, desc
from sqlalchemy.orm.exc import NoResultFound
import pytz
from . import models
from . import utils


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


class SparklineCompendiumEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            no_offset = obj.isoformat().replace('+00:00', '')
            zulu_marked = '{0}Z'.format(no_offset)
            return zulu_marked
        return json.JSONEncoder.default(self, obj)


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


def build_route_document(session, route):
    included = []
    daily_riderships = session.query(models.DailyRidership).filter_by(route_id=route.id, is_current=True)
    service_hour_riderships = session.query(models.ServiceHourRidership)\
                                                  .filter_by(route_id=route.id,
                                                             is_current=True)
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
        document = build_route_document(session, route)
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


def sort_compendium_riderships(compendiums):
    for compendium in compendiums:
        compendium['riderships'].sort(key=lambda r: r.measurement_timestamp)


def get_weekly_ridership(day_of_week, value):
    if day_of_week.lower() == 'weekday':
        return int(5 * value)
    return int(value)


def update_productivity_document(session):
    productivity = OrderedDict()
    weeklies = session.query(models.WeeklyPerformance)\
                      .order_by(desc(models.WeeklyPerformance.measurement_timestamp))\
                      .order_by(asc(models.WeeklyPerformance.productivity))
    for w in weeklies:
        ts = w.measurement_timestamp.isoformat()
        route_performance = {
            'routeNumber': w.route.route_number,
            'ridership': w.ridership,
            'productivity': w.productivity
        }
        if ts in productivity:
            productivity[ts].append(route_performance)
        else:
            productivity[ts] = [route_performance]
    productivity_series = list()
    for timestamp, route_performances in productivity.items():
        productivity_series.append({'date': timestamp, 'performance': route_performances})

    document = json.dumps(productivity_series)
    update_timestamp = datetime.datetime.now(tz=pytz.utc)
    try:
        performance_doc = session.query(models.PerformanceDocument) \
            .filter_by(name='productivity').one()
        performance_doc.document = document
        performance_doc.updated_on = update_timestamp
    except NoResultFound:
        performance_doc = models.PerformanceDocument(name='productivity',
                                                     document=document,
                                                     updated_on=update_timestamp)
        session.add(performance_doc)
    session.commit()


def update_route_sparklines(session):
    """

    Updates the JSON document with spark line data.

    The spark line JSON document is an array of route compendium dictionaries.

    =============  =================================
    Key            Value
    =============  =================================
    routeNumber    String with route's number
    roteName       String with route's name
    selector       String with a CSS selector
    data           List of spark point dictionaries
    =============  =================================

    Each *spark point* dictionary maps a ``ridership`` count and ``date`` period timestamp.

    Args:
        session: An SQLAlchemy session.
    """
    routes = session.query(models.Route).all()
    primary_data = []
    for route in routes:
        aggregator = {}
        active_ridership = session.query(models.DailyRidership).filter_by(route_id=route.id, is_current=True)
        for ridership in active_ridership:
            # UTC timezone datetime
            period_timestamp = utils.get_period_timestamp('weekday', ridership.season, ridership.calendar_year)
            pt_iso = period_timestamp.isoformat()
            if pt_iso in aggregator:
                new_count = get_weekly_ridership(ridership.day_of_week, ridership.ridership)
                old_count = aggregator[pt_iso]['ridership_count']
                aggregator[pt_iso]['ridership_count'] = old_count + new_count
            else:
                ridership_count = get_weekly_ridership(ridership.day_of_week, ridership.ridership)
                aggregator[pt_iso] = {
                    'ridership_count': ridership_count,
                    'pt': period_timestamp
                }
        for pt_iso, value in aggregator.items():
            spark_point = {'date': value['pt'], 'ridership': value['ridership_count']}
            compendium = next((c for c in primary_data if c['routeNumber'] == str(route.route_number)), None)
            if compendium:
                compendium['data'].append(spark_point)
            else:
                selector = 'ridership-sparkline-{0}'.format(str(route.route_number))
                compendium = {
                    'routeNumber': str(route.route_number),
                    'routeName': route.route_name,
                    'selector': selector,
                    'data': [spark_point]
                }
                primary_data.append(compendium)
    for compendium in primary_data:
        compendium['data'].sort(key=lambda r: r['date'])
    primary_data.sort(key=lambda c: c['data'][-1]['ridership'], reverse=True)
    document = json.dumps(primary_data, cls=SparklineCompendiumEncoder)
    update_timestamp = datetime.datetime.now(tz=pytz.utc)
    try:
        sparklines_doc = session.query(models.PerformanceDocument) \
                                .filter_by(name='ridership-sparklines').one()
        sparklines_doc.document = document
        sparklines_doc.updated_on = update_timestamp
    except NoResultFound:
        sparklines_doc = models.PerformanceDocument(name='ridership-sparklines',
                                                    document=document,
                                                    updated_on=update_timestamp)
        session.add(sparklines_doc)
    session.commit()


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
    update_route_sparklines(session)
    update_productivity_document(session)
