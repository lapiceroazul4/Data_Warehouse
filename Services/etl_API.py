import json
import logging
import os
import pandas as pd
import requests
from json import dumps, loads
from Database import Airbnbs, Airbnb_Details, Hosts, Prices, creating_engine, creating_session, closing_session, disposing_engine, db_url
from transform import airbnb_detail_dimension, fact_table_dimension, host_dimension, rename_columns, transform_price_fee_columns, transform_construction_year, transform_reviews, filling_nulls, dropping_columns, creating_city_columns, request_api, drop_duplicates_ids, creating_id_transaction, middle_table_dimension 
from kafka import KafkaProducer, KafkaConsumer
from kafka_setting import kafka_producer, kafka_consumer


# -------- Extractions -----------
credentials_directory = os.path.dirname(__file__)
credentials_path = os.path.join(credentials_directory, '../Credentials/api_config.json')

with open(credentials_path, 'r') as json_file:
    data = json.load(json_file)
    RapidAPIKey = data['X-RapidAPI-Key']
    RapidAPIHost = data['X-RapidAPI-Host']

def extract_api():
    """
    Extracts data from an API for a list of cities.

    Returns:
    - pandas.DataFrame: DataFrame containing data fetched from the API for the specified cities.
    """
    
    # --- To run ---
    """cities = [
    'New York', 'Harper Woods', 'Huntington Park', 'Oasis Spring', 'Coalville', 'Chicago', 'Seattle', 'Baltimore',
    'Ducktown', 'Mullica Hill']
    #Send the request to the API
    df = request_api(cities)"""

    script_dir1 = os.path.dirname(__file__)
    json_file1 = os.path.join(script_dir1, '../Data/df1_API.csv')

    script_dir2 = os.path.dirname(__file__)
    json_file2 = os.path.join(script_dir2, '../Data/df2_API.csv')

    df1 = pd.read_csv(json_file1, index_col=False)
    df2 = pd.read_csv(json_file2, index_col=False)
    df = pd.concat([df1,df2], ignore_index=True)
    
    return df.to_json(orient='records')

def extract_db():

    """
    Extracts data from a database table named 'Raw_Data' and converts it to a JSON-formatted output.

    Returns:
    - str: A JSON-formatted string containing data retrieved from the 'Raw_Data' table.
    """

    engine = creating_engine()
    query = "SELECT * FROM Raw_Data"
    df = pd.read_sql(query, engine)
    logging.info('Data has been extracted from db')
    return df.to_json(orient='records')

# -------- Transformation -----------
def transform_api(**kwargs):
    """
    Transforms data retrieved from an API by normalizing JSON data into a DataFrame and dropping specific columns.

    Args:
    - kwargs: the data return by the extract_api task.

    Returns:
    - str: A JSON-formatted string containing transformed data after dropping specific columns.
    """
        
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="extract_api")
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    df.drop(["usd", "measure"], inplace=True, axis="columns")

    logging.info("The data from the API has completed the transformation process")
    return df.to_json(orient='records')

def transform_db(**kwargs):
    """
    Transforms data retrieved from a database by performing a series of data processing steps on a DataFrame.

    Args:
    - kwargs: the data return by the extract_db task.

    Returns:
    - str: A JSON-formatted string containing transformed data after multiple data processing steps.
    """
        
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="extract_db")
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    df = rename_columns(df)
    df = drop_duplicates_ids(df)
    df = transform_price_fee_columns(df)
    df = transform_construction_year(df)
    df = transform_reviews(df)
    df = filling_nulls(df)
    df = dropping_columns(df)
    df = creating_city_columns(df)
    df = creating_id_transaction(df)

    return df.to_json(orient='records')

# -------- Load -----------
def creating_DWH(**kwargs):
    """
    Creates a Data Warehouse (DWH) by loading transformed data into respective tables and dimensions in a database.

    Args:
    - kwargs: the data return by the transform_api and transform_db tasks.

    Returns:
    - str: A JSON-formatted string containing records inserted into the 'airbnb_transactions' table.
    """
        
    ti = kwargs["ti"]

    str_data = ti.xcom_pull(task_ids="transform_api")
    json_data = json.loads(str_data)
    prices_table = pd.json_normalize(data=json_data)

    str_data = ti.xcom_pull(task_ids="transform_db")
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    logging.info( f"The creation of dimensions has started")
    host_table = host_dimension(df)
    airbnb_detail_table = airbnb_detail_dimension(df)
    fact = fact_table_dimension(df)
    middle_table = middle_table_dimension([
    'New York', 'Harper Woods', 'Huntington Park', 'Oasis Spring', 'Coalville', 'Chicago', 'Seattle', 'Baltimore',
    'Ducktown', 'Mullica Hill', 'Willingboro', 'Middletown', 'Houston', 'Boston', 'San Diego', 'Ravenswood', 'Seaside Heights',
    'Isla Vista', 'Beeville', 'Holderness'
])

    logging.info( f"Sending data to the database")
    engine = creating_engine()

    prices_table.to_sql('prices', engine, if_exists='replace', index=False)
    host_table.to_sql('hosts', engine, if_exists='replace', index=False)
    airbnb_detail_table.to_sql('airbnb_detail', engine, if_exists='replace', index=False)
    fact.to_sql('airbnb_transactions', engine, if_exists='replace', index=False)
    middle_table.to_sql('middle_table', engine, if_exists='replace', index=False)

    # Close the database connection
    disposing_engine(engine)
    return fact.to_json(orient='records')

def sending_kafka():
    
    """
    Initiates Kafka producer and consumer processes, to get more info about what they do, go to
    transformation.py .

    """
    kafka_producer()
    kafka_consumer()
    
