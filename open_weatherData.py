from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit
def transform_and_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="get_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])
    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (F)": temp_farenheit,
                        "Feels Like (F)": feels_like_farenheit,
                        "Minimun Temp (F)":min_temp_farenheit,
                        "Maximum Temp (F)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_Pune_' + dt_string
    df_data.to_csv(f"{dt_string}.csv", index=False)


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
        endpoint='/data/2.5/weather?q=Pune&appid=a095f3aa0708c6471a104b7388c89e14',
        poke_interval = 10
        )
        
        get_weather_data = SimpleHttpOperator(
        task_id = 'get_weather_data',
        http_conn_id = 'WeatherETL_UAT',
        endpoint='/data/2.5/weather?q=Pune&appid=a095f3aa0708c6471a104b7388c89e14',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
        )
        transform_and_load_weather_data = PythonOperator(
        task_id= 'transform_and_load_weather_data',
        python_callable=transform_and_load_data
        )
