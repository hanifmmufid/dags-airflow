from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define Python functions for the tasks
def task_1():
    print("Running Task 1: Starting process...")

def task_2():
    print("Running Task 2: Continuing process...")

def task_3():
    print("Running Task 3: Completing process...")

# Define the DAG
with DAG(
    'python_operator_dag',
    default_args=default_args,
    description='DAG with PythonOperator running Python functions',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task 1: Run a Python function
    task1 = PythonOperator(
        task_id='task_1',
        python_callable=task_1
    )

    # Task 2: Run another Python function
    task2 = PythonOperator(
        task_id='task_2',
        python_callable=task_2
    )

    # Task 3: Run another Python function
    task3 = PythonOperator(
        task_id='task_3',
        python_callable=task_3
    )

    # Define task flow (Task 1 followed by Task 2 and Task 3 running in parallel)
    task1 >> [task2, task3]
