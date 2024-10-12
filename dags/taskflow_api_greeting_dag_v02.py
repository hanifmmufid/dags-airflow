from airflow import DAG
from airflow.decorators import task
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='taskflow_api_greeting_dag_v02',
    start_date=datetime(2024, 9, 28),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Return name as a dictionary
    @task()
    def get_name():
        name_dict = {
            "first_name": "John",
            "last_name": "Doe"
        }
        print(f"Task 1 returned name: {name_dict}")
        return name_dict

    # Task 2: Return age
    @task()
    def get_age():
        age = 30
        print(f"Task 2 returned age: {age}")
        return age

    # Task 3: Return greeting message using the first and last name, and age from Task 1 and Task 2
    @task()
    def greet(name_dict: dict, age: int):
        first_name = name_dict['first_name']
        last_name = name_dict['last_name']
        greeting = f"Hello, my name is {first_name} {last_name} and I am {age} years old."
        print(greeting)
        return greeting

    # Define the task dependencies
    name_dict = get_name()
    age = get_age()
    
    # Pass name_dict and age to greet task
    greet(name_dict=name_dict, age=age)
