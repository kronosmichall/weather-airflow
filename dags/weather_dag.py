import json
from airflow.decorators import dag, task
from datetime import datetime
from tempfile import NamedTemporaryFile
import asyncio
import os

from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.kubernetes_engine import GKEAsyncHook
from weather.extract import extract as extract_weather
from weather.json_to_sql import parse_file_simple, parse_file_all


project_id = "spring-hope-450810-k2"
bucket_name = "kronosmichall-raw_weather_data"
file_path = "/tmp/data.json"

bq_dataset = "weather_dataset"
bq_table_basic = "basic_weather_info"
bq_table_all = "all_weather_info"


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
        time = current_date()
        data = extract_weather(api_key, time)
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
    

    @task(task_id="insert_into_bq_simple")
    def insert_into_bq_simple():
        data_simple = parse_file_simple(file_path)
        hook = BigQueryHook(gcp_conn_id="gcp", use_legacy_sql=False)
        
        hook.insert_all(
            project_id=project_id,
            dataset_id=bq_dataset,
            table_id=bq_table_basic,
            rows=data_simple,
        )
    insert_into_bq_simple_task = insert_into_bq_simple()
    
    
    @task(task_id="insert_into_bq_all")
    def insert_into_bq_all():
        data_all = parse_file_all(file_path)
        hook = BigQueryHook(gcp_conn_id="gcp", use_legacy_sql=False)
        hook.insert_all(
            project_id=project_id,
            dataset_id=bq_dataset,
            table_id=bq_table_all,
            rows=data_all
        )
    insert_into_bq_all_task = insert_into_bq_all()
        
        
    @task(task_id="cleanup_data")
    def cleanup_data(file_path):
        os.remove(file_path)
        return f"Removed {file_path}"
    cleanup_data_task = cleanup_data(file_path)

    
    extract_weather_task >> [send_file_task, insert_into_bq_simple_task, insert_into_bq_all_task]
    [send_file_task, insert_into_bq_simple_task, insert_into_bq_all_task] >> cleanup_data_task
        
weather_dag()