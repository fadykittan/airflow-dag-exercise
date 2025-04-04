from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'simple_python_dag',
    default_args=default_args,
    description='A simple DAG using PythonOperator',
    schedule_interval=timedelta(days=1),
    catchup=False,
)


# Define Python functions to be executed
def print_hello():
    print('Hello from Airflow!')


def get_current_date():
    print(f'Current date is: {datetime.now().strftime("%Y-%m-%d")}')


def process_data1():
    print("process_data1")


def process_data2():
    print("process_data2")


# Create tasks using PythonOperator
hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)

date_task = PythonOperator(
    task_id='date_task',
    python_callable=get_current_date,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_task',
    python_callable=process_data1,
    provide_context=True,
    dag=dag,
)

process_task2 = PythonOperator(
    task_id='process_task2',
    python_callable=process_data2,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
hello_task >> date_task >> process_task
hello_task >> date_task >> process_task2