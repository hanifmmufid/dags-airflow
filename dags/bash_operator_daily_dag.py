from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='bash_operator_daily_dag',
    start_date=datetime(2024, 10, 1),
    schedule_interval='@daily',
    catchup=True
) as dag:

    # Define the task: Echo a simple message
    echo_task = BashOperator(
        task_id='echo_hello_world',
        bash_command='echo "Hello, World!"'
    )

    # Set task dependencies (only one task in this example)
    echo_task
