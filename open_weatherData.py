from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 7),
    'email': ['ksaurabh14@outlook.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


with DAG('weatherETL',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:

        weather_api_good = HttpSensor(
        task_id ='weather_api_good',
        http_conn_id='WeatherETL_UAT',
        endpoint='/data/2.5/weather?q=Pune&appid=a095f3aa0708c6471a104b7388c89e14'
        )
