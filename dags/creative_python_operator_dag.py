from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'adventurer',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define Python functions with a creative narrative
def prepare_for_adventure():
    print("Task 1: Packing the essentials—food, water, and a map. Ready for the adventure!")

def cross_the_river(adventure_name):
    print(f"Task 2: Crossing the treacherous river during the {adventure_name} adventure. The water is cold, but we made it!")

def celebrate_victory(destination):
    print(f"Task 3: We've reached the {destination}! Time to set up camp and celebrate the success of our journey!")

# Define the DAG
with DAG(
    'creative_python_operator_dag',
    default_args=default_args,
    description='An adventurous DAG with PythonOperator and creative steps',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task 1: Preparing for the adventure
    task1 = PythonOperator(
        task_id='prepare_for_adventure',
        python_callable=prepare_for_adventure,
    )

    # Task 2: Crossing the river (with parameter)
    task2 = PythonOperator(
        task_id='cross_the_river',
        python_callable=cross_the_river,
        op_kwargs={'adventure_name': 'Epic Mountain Expedition'},
    )

    # Task 3: Celebrating at the destination (with parameter)
    task3 = PythonOperator(
        task_id='celebrate_victory',
        python_callable=celebrate_victory,
        op_kwargs={'destination': 'Summit of Mount Braveheart'},
    )

    # Task dependencies: Adventure starts with preparation, then splits into crossing and celebration
    task1 >> [task2, task3]
