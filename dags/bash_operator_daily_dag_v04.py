from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the DAG with a cron schedule interval: Tuesday to Friday at 07:30 AM
with DAG(
    dag_id='bash_operator_daily_dag_v04',
    start_date=datetime(2024, 9, 3),  # Starting from the first Tuesday of September
    schedule_interval='30 7 * * 2-5',  # Cron expression: Every Tuesday to Friday at 07:30 AM
    catchup=True
) as dag:

    # Task: Simple bash command to echo a message
    task_1 = BashOperator(
        task_id='echo_task',
        bash_command='echo "Hello, this is a daily task running from Tuesday to Friday at 07:30 AM."'
    )
