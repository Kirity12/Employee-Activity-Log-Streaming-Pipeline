import json
import logging
import requests
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka import KafkaProducer


default_orgs = {
    "owner": "admin",
    "start_date": datetime(2025, 1, 23, 00, 00, 00)
}


def fetch_and_load_json(url="http://127.0.0.1:5000/data"):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None


def stream_data():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], 
                             max_block_ms=5000,
                             api_version=(7,4,0))
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = fetch_and_load_json()
            for data in res:
                producer.send('users_created', json.dumps(data).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue


with DAG("employee_login_automation", 
         default_args=default_orgs,
         schedule='@daily',
         catchup=True) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_api',
        python_callable=(stream_data)
    )
