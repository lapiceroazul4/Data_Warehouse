import requests
import pandas as pd
import json


def extract():
    #Ciudades a consultar a la API
    ciudades = ['New York', 'Columbia', 'Baltimore','Allentown', 'New Haven', 'Washington D.C', 'San Francisco', 'Miami', 
            'Nueva Orleans', 'Seattle', 'San Diego', 'Boston', 'Chicago', 'Houston']
    #Lista en la que se agregaran las consultas posteriormente
    consultas = []
    for ciudad in ciudades:
        url = "https://cost-of-living-and-prices.p.rapidapi.com/prices"
        params = {"city_name":ciudad,"country_name":"United States"}
        headers = {
        "X-RapidAPI-Key": "a660441829msh82adfa63b5410dep1f5797jsn1b09d0c0a790",
        "X-RapidAPI-Host": "cost-of-living-and-prices.p.rapidapi.com"
        }
        print("Ya empezo el for, esta a punto de empezar el try")
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            print("Ya tengo la informacion en mi variable data")
            
            #Accediendo a la llave prices, para convertir los valores en un dataframe
            print("Accedo a la llave prices de mi diccionario en:", ciudad)
            prices = data["prices"] 

            #Creamos la columna city, y la llenamos con su valor correspondiente
            temporal_df = pd.DataFrame(prices)
            temporal_df['city'] = ciudad
            print("Creo columna city")

            #Convertimos el df en un diccionario para poder enviarlo a la lista de consultas
            diccionario_temporal = temporal_df.to_dict(orient='records')
            consultas.append(diccionario_temporal)
            print("Agrego a mi lista de diccionario")

        except requests.exceptions.RequestException as e:
            print(f"Error en la solicitud para {ciudad}: {e}")

    merged_data = {}
    print("Apunto de iniciar el ciclo para hacer merge")
    for consulta in consultas:
        merged_data.update(consulta)
    print("A punto de hacer el dataframe y hacer que la funcion lo retorne")
    df = pd.DataFrame(merged_data)
    return df

extract()
#El error es que Filadelfia no esta en la API, asi que claramente hay errores

"""def transform():
    print("Sending data into a db")   

def load():
    print("Sending data into a db")
        #prices_list = []

        for i in prices:
            prices_list.extend(i.get("price", []))

        # Crear un DataFrame con los datos de los clientes
        #df = pd.json_normalize(prices_list)

        #Guardar el dataframe en un CSV
        df.to_csv('data.csv', index=False) """