from airflow.decorators import dag, task
from datetime import datetime
import os
from weather.extract import extract as extract_weather
@dag(
    start_date=datetime(2025, 2, 19),
    schedule_interval="* * * * *",
    catchup=False
)
def weather_dag():
    @task()
    def extract():
        api_key = os.getenv("WEATHER_API_KEY") 
        result = extract_weather(api_key)