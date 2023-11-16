import json
import logging
import numpy as np
import pandas as pd
import requests
import random

# --- Transformations --- 
def rename_columns(df):

    """
    Renames the columns of a DataFrame by converting column names to lowercase 
    and replacing spaces with underscores.

    Args:
    - df (pandas.DataFrame): The DataFrame whose column names are to be renamed.

    Returns:
    - The initial DataFrame with modified column names."""

    new_column_names = [x.lower().replace(" ", "_") for x in df.columns]
    df.columns = new_column_names
    return df

def drop_duplicates_ids(df):

    """
    Drops duplicate rows in a DataFrame based on the 'id' and 'host_id' columns.

    Args:
    - df (pandas.DataFrame): The DataFrame from which duplicate rows are to be removed.

    Returns:
    - The initial DataFrame without duplicate rows based on 'id' and 'host_id'."""

    df = df.drop_duplicates(subset='id', keep='first')
    df = df.drop_duplicates(subset='host_id', keep='first')
    return df

def transform_price_fee_columns(df):

    """
    Modifies 'price' and 'service_fee' columns in a DataFrame by removing currency symbols, 
    commas, and converting them to float values.

    Args:
    - df (pandas.DataFrame): The DataFrame containing 'price' and 'service_fee' columns 
      to be transformed.

    Returns:
    - The initial DataFrame with modified 'price' and 'service_fee' columns 
      as float values without currency symbols or commas.
    """
        
    df["price"] = df["price"].str.replace('$', '').str.replace(',', '').str.strip()
    df["service_fee"] = df["service_fee"].str.replace('$', '').str.replace(',', '').str.strip()

    # Convert the columns "price" y "service_fee" to float values
    df["price"] = df["price"].astype(float)
    df["service_fee"] = df["service_fee"].astype(float)
    return df

def transform_construction_year(df):

    """
    Modifies the 'construction_year' column in a DataFrame by handling infinite values, 
    replacing NaNs with 0, and converting values to integers.

    Args:
    - df (pandas.DataFrame): The DataFrame containing the 'construction_year' column to be transformed.

    Returns:
    - The initial DataFrame with the 'construction_year' column modified,
      replacing infinite values with NaN, NaNs filled with 0, and values converted to integers.
    """
    
    df["construction_year"] = df["construction_year"].replace([np.inf, -np.inf], np.nan)
    df["construction_year"] = df["construction_year"].fillna(0)  # Rellenar valores nulos con 0 o el valor 
    df["construction_year"] = df["construction_year"].astype(int)
    return df

def transform_reviews(df):
    """
    Modifies 'last_review' and 'reviews_per_month' columns in a DataFrame 
    based on the condition of 'number_of_reviews' being equal to 0.

    Args:
    - df (pandas.DataFrame): The DataFrame to be modified.

    Returns:
    - The initial DataFrame with changes made to 'last_review' and 'reviews_per_month' columns 
      where 'number_of_reviews' equals 0.
    """
        
    df.loc[df["number_of_reviews"] == 0, "last_review"] = 0
    df.loc[df["number_of_reviews"] == 0, "reviews_per_month"] = 0
    return df

def filling_nulls(df):

    """
    Fills null values in specific columns of a DataFrame with default values.

    Args:
    - df (pandas.DataFrame): The DataFrame to handle null values.

    Returns:
    - The initial DataFrame with null values filled with the columns below.
    """
        
    df["house_rules"] = df["house_rules"].fillna("No se Especificaron Las Reglas")
    df["minimum_nights"] = df["minimum_nights"].fillna(1)
    df["instant_bookable"] = df["instant_bookable"].fillna(False)
    df["host_identity_verified"].fillna("unverified", inplace=True)
    return df

def dropping_columns(df):
    """
    Drops specified columns from a DataFrame.

    Args:
    - df (pandas.DataFrame): The DataFrame from which columns are to be dropped.

    Returns:
    - The initial DataFrame with the columns below removed.
    """
        
    df.drop(["neighbourhood_group","neighbourhood", "lat", "long"], inplace=True, axis="columns")
    return df

def creating_city_columns(df):
    """
    Creates a 'city' column in the DataFrame with randomly assigned cities id.

    Args:
    - df (pandas.DataFrame): The DataFrame to which the 'city' column will be added.

    Returns:
    - The initial DataFrame with a new 'city' column containing randomly assigned cities ids.
    """
        
    cities = [
    'New York', 'Harper Woods', 'Huntington Park', 'Oasis Spring', 'Coalville', 'Chicago', 'Seattle', 'Baltimore',
    'Ducktown', 'Mullica Hill', 'Willingboro', 'Middletown', 'Houston', 'Boston', 'San Diego', 'Ravenswood', 'Seaside Heights',
    'Isla Vista', 'Beeville', 'Holderness']
    df["city"] = np.random.choice(cities, size=len(df))
       
    return df
    # Add city column based on the city that we extract from the api
    #df["city"] = np.random.randint(1, len(cities) + 1, size=len(df))

def creating_id_transaction(df):

    """
    Creates an 'id_transaction' column and renames the existing 'id' column to 'airbnb_id' in the DataFrame.

    Args:
    - df (pandas.DataFrame): The DataFrame to modify by adding an 'id_transaction' column.

    Returns:
    - The initial DataFrame with an added 'id_transaction' column and the 'id' column renamed to 'airbnb_id'.
    """
        
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'id_transaction', 'id': 'airbnb_id'}, inplace=True)
    return df

# --- Dimensions ---
def host_dimension(df):
    """
    Creates a host dimension table from the DataFrame containing specific columns related to hosts.

    Args:
    - df (pandas.DataFrame): The DataFrame containing columns related to hosts.

    Returns:
    - pandas.DataFrame: A host dimension table containing 'host_id', 'host_name', and 'host_identity_verified'.
    """
        
    host_table = df[["host_id", "host_name", "host_identity_verified"]]
    return host_table

def airbnb_detail_dimension(df):
    """
    Creates an Airbnb detail dimension table from the DataFrame containing specific columns related to Airbnb listings.

    Args:
    - df (pandas.DataFrame): The DataFrame containing columns related to Airbnb listings.

    Returns:
    - pandas.DataFrame: An Airbnb detail dimension table containing column that are related with the details
    of each airbnb, the columns are below.
    """
        
    airbnb_detail = df[
    ["airbnb_id", "name", "instant_bookable", 
     "cancellation_policy", "room_type", "construction_year", 
     "minimum_nights", "number_of_reviews", 
     "last_review", "reviews_per_month", "review_rate_number", "calculated_host_listings_count", 
     "availability_365", "house_rules"]]
    return airbnb_detail

def fact_table_dimension(df):
    """
    Creates a fact table dimension from the DataFrame by dropping specific columns related to details except for 'airbnb_id'.

    Args:
    - df (pandas.DataFrame): The DataFrame containing columns to be dropped for the fact table.

    Returns:
    - pandas.DataFrame: A fact table dimension excluding the columns that already belong to the detail's
    dimension but retaining 'airbnb_id'.
    """
        
    df.drop(
    ["name", "instant_bookable", 
     "cancellation_policy", "room_type", "construction_year", 
     "minimum_nights", "number_of_reviews", 
     "last_review", "reviews_per_month", "review_rate_number", 
     "calculated_host_listings_count", 
     "availability_365", "house_rules", "host_name", 
     "host_identity_verified"], axis="columns", inplace=True)
    return df

def middle_table_dimension(ciudades):

    """
    Creates a middle table dimension mapping cities to their respective IDs.

    Args:
    - ciudades (list): A list of cities to be mapped.

    Returns:
    - pandas.DataFrame: A DataFrame containing a mapping of cities to their respective IDs.
    """
        
    city_dictionary = {}
    id_city = 1  

    for city in city_dictionary:
        city_dictionary[city] = id_city
        id_city += 1

    df = pd.DataFrame(list(city_dictionary.items()), columns=['city', 'id_city'])
    return df

# --- Additional Functions ---

def request_api(cities):
       
    """
    Requests cost-of-living data for multiple cities from an API and aggregates the results into a DataFrame.

    Args:
    - cities (list): A list of cities to extract from the API

    Returns:
    - pandas.DataFrame: A DataFrame containing cost-of-living data for the specified cities.
    """

    with open('Services/api_config.json', 'r') as json_file:
        data = json.load(json_file)
        RapidAPIKey = data['X-RapidAPI-Key']
        RapidAPIHost = data['X-RapidAPI-Host']
    # Create the dataframe to which we will append for each iteration
    df_result = pd.DataFrame()
    for city in cities:
        url = "https://cost-of-living-and-prices.p.rapidapi.com/prices"
        params = {"city_name": city, "country_name": "United States"}
        headers = {
        "X-RapidAPI-Key": RapidAPIKey,
        "X-RapidAPI-Host": RapidAPIHost
        }
        logging.info('')
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            logging.info(f"I am iterating in {city}")
            
            # Accessing the 'prices' key to convert the values into a dataframe
            logging.info(f"Accessing the 'prices' key in my dictionary in {city}")
            prices = data["prices"] 

            # Create the 'city' column and fill it with its corresponding value
            temporal_df = pd.DataFrame(prices)
            temporal_df['city'] = city

            # Concatenate df2 below df1
            df_result = pd.concat([df_result, temporal_df], ignore_index=True)

        except requests.exceptions.RequestException as e:
            print(f"Error in the request for {city}: {e}")
    

    return df_result