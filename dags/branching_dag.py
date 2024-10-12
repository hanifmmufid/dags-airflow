from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'branching_dag',
    default_args=default_args,
    description='DAG where Task 1 branches into Task 2 and Task 3 running in parallel',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task 1: Print Hello World
    task1 = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello from Task 1"',
    )

    # Task 2: Print message from Task 2
    task2 = BashOperator(
        task_id='say_hello_from_task_2',
        bash_command='echo "Hello from Task 2"',
    )

    # Task 3: Print message from Task 3
    task3 = BashOperator(
        task_id='say_hello_from_task_3',
        bash_command='echo "Hello from Task 3"',
    )

    # Define task flow (Task 1 followed by Task 2 and Task 3 running in parallel)
    task1 >> [task2, task3]
