from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define Python functions
def push_message():
    message = "Hello from Task 1!"
    return message  # This will automatically push the value to XCom

def pull_message(ti):
    # Pull the message from the previous task using xcom_pull
    message = ti.xcom_pull(task_ids='task_1')
    print(f"Task 2 received the message: {message}")

# Define the DAG
with DAG(
    'simple_xcom_dag',
    default_args=default_args,
    description='A simple DAG with xcom_pull',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task 1: Push message to XCom
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=push_message,
    )

    # Task 2: Pull message from XCom
    task_2 = PythonOperator(
        task_id='task_2',
        python_callable=pull_message,
    )

    # Set task dependencies
    task_1 >> task_2
