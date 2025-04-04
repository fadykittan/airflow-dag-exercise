from datetime import datetime, timedelta
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'Yair',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 10,
}

# Define the DAG
dag = DAG(
    'exmple_api',
    default_args=default_args,
    description='A simple DAG using PythonOperator and API',
    schedule_interval='@hourly',
    catchup=False,
)


def get_weather_data():
    # Construct the API URL
    url = f"https://api.open-meteo.com/v1/forecast"
    latitude = 52.52
    longitude = 13.41

    # Set up parameters
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": "temperature_2m"
    }

    try:
        # Make the API request
        response = requests.get(url, params=params)

        # Check if the request was successful
        response.raise_for_status()

        # Return the JSON response
        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        raise


def save_weather_data(**context):
    weather_data = context['ti'].xcom_pull(task_ids='weather_data')
    print(context)
    print(weather_data)
    dataset = pd.DataFrame(weather_data['current'], index=[datetime.now().isoformat()])
    dataset.to_csv('dags/data_soruce/current_weather_data.csv')


weather_data_task = PythonOperator(
    task_id='weather_data',
    python_callable=get_weather_data,
    dag=dag,
)

weather_save_data_task = PythonOperator(
    task_id='weather_save_data',
    python_callable=save_weather_data,
    dag=dag,
)

# Define task dependencies
weather_data_task >> weather_save_data_task
