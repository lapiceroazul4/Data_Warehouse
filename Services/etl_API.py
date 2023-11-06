import requests
import pandas as pd
import json
import logging
from Database import Airbnbs, Hosts, Airbnb_Details, Neighbourhoods, creating_engine, creating_session, closing_session, disposing_engine, db_url
from transform import rename_columns, transform_price_fee_columns, transform_construction_year, transform_reviews, filling_nulls, dropping_columns, creating_city_columns
from transform import host_dimension, airbnb_detail_dimension, fact_table

# -------- Extractions -----------

with open('../Credentials/api_config.json', 'r') as json_file:
    data = json.load(json_file)
    RapidAPIKey = data['X-RapidAPI-Key']
    RapidAPIHost = data['X-RapidAPI-Host']

def extract_api(cities):
    #Creamos el dataframe al que haremos append por cada iteracion
    df_result = pd.DataFrame()
    for ciudad in cities:
        url = "https://cost-of-living-and-prices.p.rapidapi.com/prices"
        params = {"city_name":ciudad,"country_name":"United States"}
        headers = {
        "X-RapidAPI-Key": RapidAPIKey,
        "X-RapidAPI-Host": RapidAPIHost
        }
        logging.info('')
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            logging.info(f"estoy iterando en {ciudad}")
            
            #Accediendo a la llave prices, para convertir los valores en un dataframe
            logging.info(f"Accedo a la llave prices de mi diccionario en {ciudad}")
            prices = data["prices"] 

            #Creamos la columna city, y la llenamos con su valor correspondiente
            temporal_df = pd.DataFrame(prices)
            temporal_df['city'] = ciudad

            # Concatenar df2 debajo de df1
            df_result = pd.concat([df_result, temporal_df], ignore_index=True)

        except requests.exceptions.RequestException as e:
            print(f"Error en la solicitud para {ciudad}: {e}")

    return df_result.to_json(orient='records')

def extract_db():
    engine = creating_engine()
    query = "SELECT * FROM Initial_Airbnbs"
    df = pd.read_sql(query, engine)
    logging.info('Data has been extracted from db')

# -------- Transformation -----------

def transform_api(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="extract_api")
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    df.drop(["usd", "measure"], inplace=True, axis="Columns")

    logging.info("the data from api has ended transformation proccess")
    return df.to_json(orient='records')

def transform_db(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="extract_db")
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    df = rename_columns(df)
    df = transform_price_fee_columns(df)
    df = transform_construction_year(df)
    df = transform_reviews(df)
    df = filling_nulls(df)
    df = dropping_columns(df)
    df = creating_city_columns(df)

    return df.to_json(orient='records')

# -------- Load -----------
def creating_DWH(**kwargs):
    ti = kwargs["ti"]

    str_data = ti.xcom_pull(task_ids="transform_api")
    json_data = json.loads(str_data)
    prices_table = pd.json_normalize(data=json_data)

    str_data = ti.xcom_pull(task_ids="transform_db")
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

    logging.info( f"the creation of dimensions has started")
    host_table = host_dimension(df)
    airbnb_detail_table = airbnb_detail_dimension(df)
    fact = fact_table(df)

    logging.info( f"sending data to db")
    engine = creating_engine()

    prices_table.to_sql('prices', engine, if_exists='replace', index=False)
    host_table.to_sql('hosts', engine, if_exists='replace', index=False)
    airbnb_detail_table.to_sql('details', engine, if_exists='replace', index=False)
    fact.to_sql('airbnbs', engine, if_exists='replace', index=False)

    #Cerramos la conexion a la db
    disposing_engine(engine)

def sending_kafka(**kwargs):
    pass
#TODO: Como voy a enviar datos a kafka cuando cada dato corresponde a un dataframe diferente?
# En caso de que se envie el dataframe inicial entonces si es necesario hacer un merge