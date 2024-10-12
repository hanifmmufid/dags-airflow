from airflow import DAG
from airflow.decorators import task
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='taskflow_api_greeting_dag',
    start_date=datetime(2024, 9, 28),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Return name
    @task()
    def get_name():
        name = "John Doe"
        print(f"Task 1 returned name: {name}")
        return name

    # Task 2: Return age
    @task()
    def get_age():
        age = 30
        print(f"Task 2 returned age: {age}")
        return age

    # Task 3: Return greeting message using the name and age from Task 1 and Task 2
    @task()
    def greet(name: str, age: int):
        greeting = f"Hello, my name is {name} and I am {age} years old."
        print(greeting)
        return greeting

    # Define the task dependencies
    name = get_name()
    age = get_age()
    greet(name=name, age=age)
