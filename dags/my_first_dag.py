from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Definisikan default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Inisiasi DAG
with DAG(
    dag_id='my_first_dag',
    default_args=default_args,
    description='DAG pertama saya di Airflow',
    schedule_interval='@daily',
    start_date=datetime(2024, 9, 28), 
    catchup=False,
) as dag:

    # Task 1: Cetak Hello
    task1 = BashOperator(
        task_id='hello_world',
        bash_command='echo "Hello World"',
    )

    # Task 2: Cetak Goodbye
    task2 = BashOperator(
        task_id='goodbye_world',
        bash_command='echo "Goodbye World"',
    )

    # Definisikan urutan task
    task1 >> task2
