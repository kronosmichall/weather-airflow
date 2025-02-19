import json
from airflow.decorators import dag, task
from datetime import datetime
from tempfile import NamedTemporaryFile
import asyncio
import os
from weather.extract import extract as extract_weather
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
project_id = "spring-hope-450810-k2"
bucket_name = "kronosmichall-raw_weather_data"
file_path = "/tmp/data.json"

def current_date():
    return datetime.now().strftime("%Y-%m-%d %H:%M")

@dag(
    start_date=datetime(2025, 2, 19),
    schedule_interval="* * * * *",
    catchup=False
)
def weather_dag():
    @task(task_id="extract", retries=2)
    def extract():
        api_key = os.getenv("WEATHER_API_KEY")
        data = extract_weather(api_key)
        with open(file_path, "w") as f:
            json.dump(data, f)

        return f.name

    extract_weather_task = extract()


    send_file_task = LocalFilesystemToGCSOperator(
        src=file_path,
        dst=f"{current_date()}.json",
        bucket=bucket_name,
        task_id="send_file",
        gcp_conn_id="gcp",
        mime_type="application/json",
    )

    
    @task(task_id="cleanup_data")
    def cleanup_data(file_path):
        os.remove(file_path)
        return f"Removed {file_path}"
    
    cleanup_data_task = cleanup_data(file_path)

    
    extract_weather_task >> send_file_task >> cleanup_data_task
        
weather_dag()