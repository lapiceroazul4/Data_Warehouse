import json
import os
import psycopg2
from sqlalchemy import create_engine, Column, Integer, Float, String, Text, BigInteger, Date, ForeignKey, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Read db configuration from JSON
# -------- Extractions -----------
credentials_directory = os.path.dirname(__file__)
credentials_path = os.path.join(credentials_directory, '../Credentials/pg_config.json')

with open(credentials_path, 'r') as json_file:
    data = json.load(json_file)
    user = data["user"]
    password = data["password"]
    database = data["database"]
    server = data["server"]
    charset = "utf-8"

db_url = f"postgresql://{user}:{password}@{server}/{database}"
Base = declarative_base()

class Airbnbs(Base):
    __tablename__ = 'airbnb_transactions'
    id_transaction = Column(Integer, primary_key=True)
    airbnb_id = Column(BigInteger, ForeignKey('airbnb_detail.id'))
    host_id = Column(BigInteger, ForeignKey('hosts.host_id'))
    price = Column(Float)
    service_fee = Column(Float)
    city = Column(String)
    
    def __init__(self, session, engine):
        self.session = session
        Base.metadata.create_all(engine)

    # Create a new transaction, new_airbnb should bring all the information preferably in a dictionary.
    def create(self, new_airbnb):
        self.session.add(new_airbnb)
        self.session.commit()

    # Update an Airbnb Transaction, update_data must bring all the information to update
    def update(self, update_data):
        for key, value in update_data.items():
            setattr(self, key, value)
        self.session.commit()

    # Delete rows, requires Airbnb ID
    def delete(self, airbnb_id):
        self.session.delete(airbnb_id)
        self.session.commit()

class Hosts(Base):
    __tablename__ = 'hosts'
    host_id = Column(BigInteger, primary_key=True)
    host_name = Column(String)
    host_identity_verified = Column(String)

    def __init__(self, session, engine):
        self.session = session
        Base.metadata.create_all(engine)

    # Create a new Host, new_host should bring all the information preferably in a dictionary.
    def create(self, new_host):
        self.session.add(new_host)
        self.session.commit()

    # Update an Airbnb, update_data must bring all the information to update
    def update(self, update_data):
        for key, value in update_data.items():
            setattr(self, key, value)
        self.session.commit()

    # Delete rows, requires Host ID
    def delete(self, host_id):
        self.session.delete(host_id)
        self.session.commit()

class Prices(Base): 
    __tablename__ = 'prices'
    index = Column(Integer, primary_key=True)
    good_id = Column(Integer)
    item_name = Column(String)
    category_id = Column(Integer)
    category_name = Column(String)
    min = Column(Float)
    avg = Column(Float)
    max = Column(Float)
    city = Column(String)


class Airbnb_Details(Base):
    __tablename__ = 'airbnb_detail'
    id = Column(BigInteger, primary_key=True)
    name = Column(String)
    instant_bookable = Column(String)
    cancellation_policy = Column(String)
    room_type = Column(Text)
    construction_year = Column(Integer)
    minimum_nights = Column(Integer)
    number_of_reviews = Column(Integer)
    last_review = Column(Date)
    reviews_per_month = Column(Float)
    review_rate_number = Column(Float)
    calculated_host_listings_count = Column(Integer)
    availability_365 = Column(Integer)
    house_rules = Column(Text)

    def __init__(self, session, engine):
        self.session = session
        Base.metadata.create_all(engine)

    # Create a new Airbnb_Detail, Airbnb_Detail should bring all the information preferably in a dictionary.
    def create(self, Airbnb_Detail):
        self.session.add(Airbnb_Detail)
        self.session.commit()

    # Update an Airbnb Detail, Airbnb_Detail must bring all the information to update
    def update(self, update_data):
        for key, value in update_data.items():
            setattr(self, key, value)
        self.session.commit()

    # Delete rows, requires Airbnb ID
    def delete(self, id):
        self.session.delete(id)
        self.session.commit()

# Function to Create Engine
def creating_engine():
    engine = create_engine(db_url)
    return engine

# Function to create the sessions
def creating_session(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

# Function to close the session
def closing_session(session):
    session.close()

# Function to Dispose Engine
def disposing_engine(engine):
    engine.dispose()
    print("engine closed")
