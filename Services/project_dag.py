
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl_API import extract_api, extract_db, transform_api, transform_db, creating_DWH, sending_kafka

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 8),  # Update the start date to today or an appropriate date
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'ETL Project',
    default_args=default_args,
    description='ETL Project',
    schedule_interval='@daily',  # Set the schedule interval as per your requirements
) as dag:

    read_db = PythonOperator(
        task_id='extract_db',
        python_callable=extract_db,
    )

    transform_db = PythonOperator(
        task_id='transform_db',
        python_callable=transform_db,
        )
    
    read_csv = PythonOperator(
        task_id='extract_api',
        python_callable=extract_api,
    )

    transform_csv = PythonOperator(
        task_id='transform_api',
        python_callable=transform_api,
        )
    
    merge = PythonOperator(
        task_id='creating_DWH',
        python_callable=creating_DWH,
    )

    load = PythonOperator(
        task_id='sending_kafka',
        python_callable=sending_kafka,
    )

    
    extract_db >> transform_db >> creating_DWH
    extract_api >> transform_api >> creating_DWH >> sending_kafka 