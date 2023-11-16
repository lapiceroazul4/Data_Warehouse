import time
import pandas as pd
import requests
from kafka import KafkaProducer, KafkaConsumer
from json import dumps, loads
from Database import Base, creating_engine, creating_session, closing_session, disposing_engine
from joblib import load



kafka_topic = "etl_project"
def kafka_producer():
        producer = KafkaProducer(
        value_serializer=lambda m: dumps(m).encode('utf-8'),
        bootstrap_servers=['localhost:9092'],
    )

        # Create the engine and session
        engine = creating_engine()
        session = creating_session(engine)

        try:
            # SQL Query
            sql_query = "SELECT * FROM airbnb_transactions;"

            # Read data from the database to a DataFrame
            df = pd.read_sql_query(sql_query, engine)

            for _, row in df.iterrows():
                message = {
                    "id_transaction": row["id_transaction"],
                    "airbnb_id": row["airbnb_id"],
                    "host_id": row["host_id"],
                    "price": row["price"],
                    "service_fee": row["service_fee"],
                    "city": row["city"]
                }
                producer.send(kafka_topic, value=message)
                print("Sending messages from producer")
                # time.sleep(2)

        except Exception as e:
            print(f"Error processing and sending data: {str(e)}")

        # Close the engine and session
        closing_session(session)
        disposing_engine(engine)

API_ENDPOINT = "https://api.powerbi.com/beta/693cbea0-4ef9-4254-8977-76e05cb5f556/datasets/878d4322-4003-4d6f-80f0-8e77a154cdd1/rows?experience=power-bi&key=lMu%2B2rSo4%2FR9XaTnUL0ozbm7rcyi8X8FABSypxo%2BokaEIzUbqaKqC9F%2BsNw0CB3X6yox8SjfMcwkisabmXaWgw%3D%3D"

def kafka_consumer():
        consumer = KafkaConsumer(
            'etl_project',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group-1',
            value_deserializer=lambda m: loads(m.decode('utf-8')),
            bootstrap_servers=['localhost:9092'],
            max_poll_interval_ms=30000
        )

        for m in consumer:
            message = m.value
            print(type(message))
            # Convert the message into a DataFrame
            df = pd.DataFrame([message])
            data = bytes(df.to_json(orient='records'), 'utf-8')
            # Send the message to the specified API_ENDPOINT
            try:
                response = requests.post(API_ENDPOINT, data)
                response.raise_for_status()
                print(f"Message sent successfully: {response.text}")
            except requests.exceptions.RequestException as e:
                print(f"Error sending message: {str(e)}")
            time.sleep(2)