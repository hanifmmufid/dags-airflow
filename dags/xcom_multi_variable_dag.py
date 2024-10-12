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
def push_multiple_values():
    # Push multiple values to XCom by returning a dictionary
    return {
        'name': 'Alice',
        'age': 30,
        'location': 'Wonderland'
    }

def pull_multiple_values(ti):
    # Pull multiple values from XCom using task_ids='task_1'
    xcom_values = ti.xcom_pull(task_ids='task_1')
    
    # Extract each value from the XCom dictionary
    name = xcom_values['name']
    age = xcom_values['age']
    location = xcom_values['location']
    
    print(f"Name: {name}, Age: {age}, Location: {location}")

# Define the DAG
with DAG(
    'xcom_multi_variable_dag',
    default_args=default_args,
    description='A DAG demonstrating XCom with multiple variables',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task 1: Push multiple values to XCom
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=push_multiple_values,
    )

    # Task 2: Pull multiple values from XCom and print them
    task_2 = PythonOperator(
        task_id='task_2',
        python_callable=pull_multiple_values,
    )

    # Set task dependencies
    task_1 >> task_2
