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

# Task 1: Generate a value
def generate_value():
    value = 42
    print(f"Task 1 generated value: {value}")
    return value  # Pushed to XCom

# Task 2: Pull the value from Task 1, process it, and push a new value
def process_value(ti):
    value = ti.xcom_pull(task_ids='task_1')
    new_value = value * 2  # Simple processing: doubling the value
    print(f"Task 2 processed value: {new_value}")
    return new_value  # Pushed to XCom

# Task 3: Pull the processed value from Task 2 and print it
def output_value(ti):
    final_value = ti.xcom_pull(task_ids='task_2')
    print(f"Task 3 final value: {final_value}")

# Define the DAG
with DAG(
    'xcom_value_chain_dag',
    default_args=default_args,
    description='A DAG with multiple tasks passing values using XCom',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task 1: Generate a value
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=generate_value,
    )

    # Task 2: Process the value from Task 1
    task_2 = PythonOperator(
        task_id='task_2',
        python_callable=process_value,
    )

    # Task 3: Output the final value from Task 2
    task_3 = PythonOperator(
        task_id='task_3',
        python_callable=output_value,
    )

    # Define task dependencies
    task_1 >> task_2 >> task_3
