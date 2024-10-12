from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the DAG with a cron schedule interval
with DAG(
    dag_id='bash_operator_daily_dag_v02',
    start_date=datetime(2024, 9, 28),
    schedule_interval='30 7 * * *',  # Cron expression: Every day at 07:30 AM
    catchup=True
) as dag:

    # Task 1: Echo the name (John Doe)
    task_1 = BashOperator(
        task_id='echo_name',
        bash_command='echo "John Doe"'
    )

    # Task 2: Echo the age (30)
    task_2 = BashOperator(
        task_id='echo_age',
        bash_command='echo "30"'
    )

    # Task 3: Echo the greeting message using name and age
    task_3 = BashOperator(
        task_id='echo_greeting',
        bash_command='echo "Hello, my name is John Doe and I am 30 years old."'
    )

    # Define the task dependencies
    task_1 >> task_2 >> task_3
