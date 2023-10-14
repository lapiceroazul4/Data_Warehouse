import json
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean, Date, Text, ForeignKey, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

#Leer config desde el JSON
with open('db_config.json', 'r') as json_file:
    data = json.load(json_file)
    usuario = data["user"]
    password = data["passwd"]
    server = data["server"]
    database = data["database"]
    charset = "utf-8"

db_url = f"mysql+pymysql://{usuario}:{password}@{server}/{database}?charset={charset}"
Base = declarative_base()


class Airbnbs(Base):
    __tablename__ = 'airbnb_transactions'
    id_transaction = Column(Integer, primary_key=True)
    airbnb_id = Column(Integer, ForeignKey('airbnb_detail.id'))
    host_id = Column(Integer, ForeignKey('hosts.host_id'))
    neighbourhood_id = Column(Integer, ForeignKey('neighbourhoods.neighbourhood_id'))

    def __init__ (self, session, engine):
        self.session = session
        Base.metadata.create_all(engine)

    #Create a new transaction, new_airbnb should bring all the information preferably in a dictionary.
    def create(self, new_airbnb):
        self.session.add(new_airbnb)
        self.session.commit()

    #Update an Airbnb Transaction, update_data must bring all the information to update
    def update(self, update_data):
        for key, value in update_data.items():
            setattr(self, key, value)
        self.session.commit()

    #Delete rows, requires Airbnb ID
    def delete(self, airbnb_id):
        self.session.delete(airbnb_id)
        self.session.commit()

class Hosts(Base):

    __tablename__ = 'hosts'

    host_id = Column(Integer, primary_key=True)
    host_name = Column(String)
    host_identity_verified = Column(VARCHAR(10))

    def __init__ (self, session, engine):
        self.session = session
        Base.metadata.create_all(engine)

    #Create a new Host, new_hostshould bring all the information preferably in a dictionary.
    def create(self, new_host):
        self.session.add(new_host)
        self.session.commit()

    #Update an Airbnb, update_data must bring all the information to update
    def update(self, update_data):
        for key, value in update_data.items():
            setattr(self, key, value)
        self.session.commit()

    #Delete rows, requires Host ID
    def delete(self, host_id):
        self.session.delete(host_id)
        self.session.commit()

class Airbnb_Details(Base):
    __tablename__ = 'airbnb_detail'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    instant_bookable = Column(String)
    cancellation_policy = Column(String)
    roomtype = Column(String)
    construction_year = Column(Integer)
    price = Column(Float)
    service_fee = Column(Float)
    minimum_nights = Column(Integer)
    number_of_reviews = Column(Integer)
    last_review = Column(Date)
    reviews_per_month = Column(Float)
    review_rate_number = Column(Float)
    calculated_host_listings_count = Column(Integer)
    availability_365 = Column(Integer)
    house_rules = Column(String)

    def __init__ (self, session, engine):
        self.session = session
        Base.metadata.create_all(engine)

    #Create a new Airbnb_Detail, Airbnb_Detail should bring all the information preferably in a dictionary.
    def create(self, Airbnb_Detail):
        self.session.add(Airbnb_Detail)
        self.session.commit()

    #Update an Airbnb Detail, Airbnb_Detail must bring all the information to update
    def update(self, update_data):
        for key, value in update_data.items():
            setattr(self, key, value)
        self.session.commit()

    #Delete rows, requires Airbnb ID
    def delete(self, id):
        self.session.delete(id)
        self.session.commit()

class Neighbourhoods(Base):
    __tablename__ = 'neighbourhoods'

    neighbourhood_id = Column(Integer, primary_key=True)
    neighbourhood_group = Column(String)
    neighbourhood = Column(String)
    lat = Column(Float)
    long = Column(Float)

    def __init__ (self, session, engine):
        self.session = session
        Base.metadata.create_all(engine)

    #Create a new Neighbor, Neighbor should bring all the information preferably in a dictionary.
    def create(self, Neighbor):
        self.session.add(Neighbor)
        self.session.commit()

    #Update an Neighbor, update_Neighbor must bring all the information to update
    def update(self, update_Neighbor):
        for key, value in update_Neighbor.items():
            setattr(self, key, value)
        self.session.commit()

    #Delete rows, requires Neighbor_id
    def delete(self, neighbourhood_id):
        self.session.delete(neighbourhood_id)
        self.session.commit()

#Function to Create Engine
def creating_engine():
    engine = create_engine(db_url)
    return engine

#Function to create the sessions
def creating_session(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

#Function to close the session
def closing_session(session):
    session.close()