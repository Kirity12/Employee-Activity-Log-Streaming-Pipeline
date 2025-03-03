import json
import logging
import requests
import time
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic

log_filename = 'logs/airflow_dag.log'
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()  # Logs also to console
    ]
)


# Custom failure callback to log success
def on_failure_callback(**context):
    task_id = context.get('task_instance').task_id
    error_message = f"Task {task_id} failed"
    # Send an email alert
    print(f'''
    send_slack_webhook(
        channel=UserLoginDagFailures,
        subject=f"Airflow Task {task_id} Failed",
        html_content={error_message}
    )
    ''')

# Custom success callback to log success
def on_success_callback(**context):
    task_id = context.get('task_instance').task_id
    success_message = f"Task {task_id} succeeded"
    # Log success (can be extended to Slack, or other platforms)
    print(success_message)


default_orgs = {
    "owner": "admin",
    "start_date": datetime(2025, 1, 23, 00, 00, 00),
    'on_failure_callback': on_failure_callback,
    'on_success_callback': on_success_callback,
}

TOPIC_NAME = "users_created"

def fetch_and_load_json(url="http://127.0.0.1:5000/data"):
    "fetch json response from API"
    try:
        response = requests.get(url)
        response.raise_for_status()  
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None


def test_kafka_connection(**context):
    """Verify Kafka connection inside Docker"""
    task_id = context.get('task_instance').task_id
    try:
        admin = KafkaAdminClient(bootstrap_servers='broker:29092', request_timeout_ms=5000)
        topics = admin.list_topics()
        logging.info(f"Task {task_id}: Kafka topics found: {topics}")
        context['task_instance'].xcom_push(key='kafka_connection_status', value='success')
        return 'create_topic'
    except Exception as e:
        logging.error(f"Task {task_id}: Kafka connection failed: {e}")
        context['task_instance'].xcom_push(key='kafka_connection_status', value='failure')
        return 'slack_send_alert'


def process_kafka_connection_result(**context):
    # Pull the Kafka connection status from XCom
    kafka_connection_status = context['task_instance'].xcom_pull(task_ids='branch_task', key='kafka_connection_status')
    task_id = context.get('task_instance').task_id
    if kafka_connection_status == 'success':
        logging.info(f"Task {task_id}: Kafka connection was successful. Proceeding with topic creation and streaming.")
    elif kafka_connection_status == 'failure':
        logging.error(f"Task {task_id}: Kafka connection failed. Aborting further steps.")
    else:
        logging.warning(f"Task {task_id}: No Kafka connection status found in XCom.")


def create_topic(**context):
    """Create Kafka topic if it doesn't exist"""
    task_id = context.get('task_instance').task_id
    try:
        admin_client = KafkaAdminClient(bootstrap_servers='broker:29092', request_timeout_ms=5000)
        existing_topics = admin_client.list_topics()
        topic_name = TOPIC_NAME

        if topic_name not in existing_topics:
            admin_client.create_topics(new_topics=[NewTopic(name=topic_name, num_partitions=1, replication_factor=1)])
            logging.info(f"Task {task_id}: Topic '{topic_name}' created successfully")
        else:
            logging.info(f"Task {task_id}: Topic '{topic_name}' already exists")
    except Exception as e:
        logging.error(f"Task {task_id}: Kafka topic creation failed: {e}")
        raise


def stream_data(**context):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], 
                             max_block_ms=5000,
                             api_version=(7,4,0))
    task_id = context.get('task_instance').task_id
    curr_time = time.time()

    while time.time() < curr_time + 120:  # Fixed condition here
        try:
            res = fetch_and_load_json()
            for data in res:
                producer.send('users_created', json.dumps(data).encode('utf-8'))

            logging.info(f"Task {task_id}: Data Production to Kafka succefully completed")
        except Exception as e:
            logging.error(f'Task {task_id}: An error occurred: {e}')
            continue


with DAG("employee_login_automation", 
         default_args=default_orgs,
         schedule='@daily',
         catchup=True) as dag:

    start_dag = EmptyOperator(task_id='start_dag', dag=dag)
    end_dag = EmptyOperator(task_id='end_dag', dag=dag)

    create_kafka_topic = PythonOperator(
                        task_id='create_topic',
                        python_callable=create_topic)
    
    slack_alert_task = BashOperator(
                        task_id="slack_send_alert",
                        bash_command="echo ******************************KAFKA NOT FOUND******************************",
                    )
    
    branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=test_kafka_connection,
    dag=dag)

    streaming_task = PythonOperator(
                        task_id='stream_data_api',
                        python_callable=stream_data)
    
    process_connection = PythonOperator(
                            task_id='process_kafka_connection_result',
                            python_callable=process_kafka_connection_result)  # Fixed task id here

    # Set dependencies
    start_dag >> branch_task
    branch_task >> [create_kafka_topic, slack_alert_task]
    create_kafka_topic >> process_connection
    process_connection >> streaming_task
    streaming_task >> end_dag
    slack_alert_task >> end_dag
