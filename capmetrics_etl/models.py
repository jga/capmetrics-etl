from sqlalchemy import Boolean, Column, Integer, Float, DateTime, ForeignKey, String
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Route(Base):
    __tablename__ = 'route'
    id = Column(Integer, primary_key=True)
    route_number = Column(Integer, unique=True)
    route_name = Column(String)
    service_type = Column(String)


class DailyRidership(Base):
    __tablename__ = 'daily_ridership'
    id = Column(Integer, primary_key=True)
    created_on = Column(DateTime)
    current = Column(Boolean)
    day_of_week = Column(String)
    season = Column(String)
    year = Column(Integer)
    ridership = Column(Float)
    route_id = Column(Integer, ForeignKey('route.id'))
    route = relationship("Route", backref='daily_ridership')


class HourlyRidership(Base):
    __tablename__ = 'hourly_ridership'
    id = Column(Integer, primary_key=True)
    created_on = Column(DateTime)
    current = Column(Boolean)
    day_of_week = Column(String)
    season = Column(String)
    year = Column(Integer)
    ridership = Column(Float)
    route_id = Column(Integer, ForeignKey('route.id'))
    route = relationship("Route", backref='hourly_ridership')

