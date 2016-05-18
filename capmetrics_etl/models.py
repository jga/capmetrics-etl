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
    season_timestamp = Column(DateTime(timezone=True))


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
    season_timestamp = Column(DateTime(timezone=True))


class ETLReport(Base):
    """Captures basic metrics for an ETL job."""
    __tablename__ = 'etl_report'
    id = Column(Integer, primary_key=True)
    etl_type = Column(String)
    created_on = Column(DateTime(timezone=True))
    creates = Column(Integer)
    updates = Column(Integer)
    total_models = Column(Integer)

