"""
The **capmetrics-etl** models represent groups of data that allow for
consistent comparisons of ridership and other performance data across
time, routes, and service types.
"""
from sqlalchemy import Boolean, Column, Integer, Float, DateTime, ForeignKey, String
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Route(Base):
    """
    A geographically semi-consistent designation for transit service.
    """
    __tablename__ = 'route'
    id = Column(Integer, primary_key=True)
    route_number = Column(Integer, unique=True)
    route_name = Column(String)
    service_type = Column(String)


class DailyRidership(Base):
    """
    Estimated "daily" ridership for a type of day (weekday, Saturday, Sunday)
    and season.
    """
    __tablename__ = 'daily_ridership'
    id = Column(Integer, primary_key=True)
    created_on = Column(DateTime(timezone=True))
    is_current = Column(Boolean)
    day_of_week = Column(String)
    season = Column(String)
    calendar_year = Column(Integer)
    ridership = Column(Float)
    route_id = Column(Integer, ForeignKey('route.id'))
    route = relationship("Route", backref='daily_ridership')
    measurement_timestamp = Column(DateTime(timezone=True))


class ServiceHourRidership(Base):
    """
    Estimated service hour productivity for a type of day (weekday, Saturday, Sunday)
    and season.
    """
    __tablename__ = 'service_hour_ridership'
    id = Column(Integer, primary_key=True)
    created_on = Column(DateTime(timezone=True))
    is_current = Column(Boolean)
    day_of_week = Column(String)
    season = Column(String)
    calendar_year = Column(Integer)
    ridership = Column(Float)
    route_id = Column(Integer, ForeignKey('route.id'))
    route = relationship("Route", backref='service_hour_ridership')
    measurement_timestamp = Column(DateTime(timezone=True))


class SystemRidership(Base):
    """
    Estimated **system-wide** ridership for a (1) type of day (weekday, Saturday, Sunday)
    by (2) season and (3) service type.
    """
    __tablename__ = 'system_ridership'
    id = Column(Integer, primary_key=True)
    calendar_year = Column(Integer)
    created_on = Column(DateTime(timezone=True))
    day_of_week = Column(String)
    is_active = Column(Boolean)
    ridership = Column(Float)
    season = Column(String)
    measurement_timestamp = Column(DateTime(timezone=True))
    service_type = Column(String)


class SystemTrend(Base):
    """
    Aggregates ``SystemRidership`` facts to conveniently provide a performance
    trend for a service type across season-based measurements.

    Attributes:
        id: An integer primary key.
        service_type: A string column with names for service types.
        trend: A JSON string with longitudinal ridership data.
        updated_on: A timezone-aware datetime.
    """
    __tablename__ = 'system_trend'
    id = Column(Integer, primary_key=True)
    service_type = Column(String)
    trend = Column(String)
    updated_on = Column(DateTime(timezone=True))


class ETLReport(Base):
    """Captures basic metrics for an ETL job."""
    __tablename__ = 'etl_report'
    id = Column(Integer, primary_key=True)
    etl_type = Column(String)
    created_on = Column(DateTime(timezone=True))
    creates = Column(Integer)
    updates = Column(Integer)
    total_models = Column(Integer)

