from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl_API import creating_DWH, extract_api, extract_db, sending_kafka, transform_api, transform_db

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 9),  # Update the start date to today or an appropriate date
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'ETL_Project',
    default_args=default_args,
    description='ETL_Project',
    schedule_interval='@daily',  # Set the schedule interval as per your requirements
) as dag:

    extract_db = PythonOperator(
        task_id='extract_db',
        python_callable=extract_db,
    )

    transform_db = PythonOperator(
        task_id='transform_db',
        python_callable=transform_db,
        )
    
    extract_api = PythonOperator(
        task_id='extract_api',
        python_callable=extract_api,
    )

    transform_api = PythonOperator(
        task_id='transform_api',
        python_callable=transform_api,
        )
    
    creating_DWH = PythonOperator(
        task_id='creating_DWH',
        python_callable=creating_DWH,
    )

    sending_kafka = PythonOperator(
        task_id='sending_kafka',
        python_callable=sending_kafka,
    )

    extract_db >> transform_db >> creating_DWH
    extract_api >> transform_api >> creating_DWH >> sending_kafka 