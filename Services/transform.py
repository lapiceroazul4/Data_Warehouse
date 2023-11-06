import numpy as np


def rename_columns(df):
    new_column_names = [x.lower().replace(" ", "_") for x in df.columns]
    df.columns = new_column_names
    return df

def transform_price_fee_columns(df):
    df["price"] = df["price"].str.replace('$', '').str.replace(',', '').str.strip()
    df["service_fee"] = df["service_fee"].str.replace('$', '').str.replace(',', '').str.strip()

    # Convertir las columnas "price" y "service_fee" a valores flotantes
    df["price"] = df["price"].astype(float)
    df["service_fee"] = df["service_fee"].astype(float)
    return df

def transform_construction_year(df):
    df["construction_year"] = df["construction_year"].replace([np.inf, -np.inf], np.nan)
    df["construction_year"] = df["construction_year"].fillna(0)  # Rellenar valores nulos con 0 o el valor 
    df["construction_year"] = df["construction_year"].astype(int)
    return df

def transform_reviews(df):
    df.loc[df["number_of_reviews"] == 0, "last_review"] = 0
    df.loc[df["number_of_reviews"] == 0, "reviews_per_month"] = 0
    return df

def filling_nulls(df):
    df["house_rules"] = df["house_rules"].fillna("No se Especificaron Las Reglas")
    df["minimum_nights"] = df["minimum_nights"].fillna(1)
    df["instant_bookable"] = df["instant_bookable"].fillna(False)
    df["host_identity_verified"].fillna("unverified", inplace=True)
    return df

def dropping_columns(df):
    df.drop(["neighbourhood_group","neighbourhood", "lat", "long"], inplace=True, axis="Columns")
    return df

def creating_city_columns(df):
    cities = [
    'New York', 'Harper Woods', 'Huntington Park', 'Oasis Spring', 'Coalville', 'Chicago', 'Seattle', 'Baltimore',
    'Ducktown', 'Mullica Hill', 'Willingboro', 'Middletown', 'Houston', 'Boston', 'San Diego', 'Ravenswood', 'Seaside Heights',
    'Isla Vista', 'Beeville', 'Holderness']
    # Agregar una columna 'City' con valores aleatorios
    df["city"] = np.random.choice(cities, size=len(df))
    return df

def host_dimension(df):
    host_table = df[["host_id", "host_name", "host_identity_verified"]]
    return host_table

def airbnb_detail_dimension(df):
    airbnb_detail = df[
    ["id", "name", "instant_bookable", 
     "cancellation_policy", "room_type", "construction_year", 
     "minimum_nights", "number_of_reviews", 
     "last_review", "reviews_per_month", "review_rate_number", "calculated_host_listings_count", 
     "availability_365", "house_rules"]]
    return airbnb_detail

def fact_table(df):
    df.drop(
    ["id", "name", "instant_bookable", 
     "cancellation_policy", "room_type", "construction_year", 
     "minimum_nights", "number_of_reviews", 
     "last_review", "reviews_per_month", "review_rate_number", 
     "calculated_host_listings_count", 
     "availability_365", "house_rules", "host_name", 
     "host_identity_verified"], axis=1, inplace=True)
    return df