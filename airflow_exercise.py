from datetime import datetime, timedelta, date
from airflow import DAG
import pandas as pd
from airflow.operators.python_operator import PythonOperator
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'Fady',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['fkittan@salesforce.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': 1,
}


# Define the DAG
dag = DAG(
    'fady_dag_1',
    default_args=default_args,
    description='My first Airflow DAG',
    schedule_interval='*/5 * * * *',
    catchup=False,
)


def read_file():
    print(os.listdir(os.getcwd()))
    print(os.getcwd())
    df = pd.read_csv("dags/data/in/Iris.csv")
    print(df.head())
    return df

def transform(**res):
    # Use XCom to push the result
    ti = res['ti']
    df = ti.xcom_pull(task_ids='read_file')

    df["sepal_area"] = df["SepalLengthCm"] * df["SepalWidthCm"]
    df["petal_area"] = df["PetalLengthCm"] * df["PetalWidthCm"]

    convert_map = {
        "Iris-setosa": 1,
        "Iris-versicolor": 2,
        "Iris-virginica": 3
    }
    df['Species'] = df['Species'].map(convert_map)
    return df

def load_data(**res):
    ti = res['ti']
    df = ti.xcom_pull(task_ids='transform')

    today = date.today()
    df.to_csv("dags/data/out/Iris_" + str(today) + ".csv")


read_file = PythonOperator(
    task_id='read_file',
    python_callable=read_file,
    provide_context=True,
    dag=dag
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag
)

load_file = PythonOperator(
    task_id='load_file',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)


read_file >> transform >> load_file
