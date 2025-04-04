from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': '30 10 * * 0',
}

# Define the DAG
dag = DAG(
    'exmple_dag_1',
    default_args=default_args,
    description='A simple DAG using PythonOperator',
    schedule_interval=timedelta(days=1),
    catchup=False,
)


def transform_sales_data(ti):
    try:
        sales_data = pd.read_csv('dags/data_soruce/sales_data.csv')

        # Data transformation logic
        # 1. Convert order_date column to datetime
        sales_data['order_date'] = pd.to_datetime(sales_data['order_date'])

        # 2. Calculate the total revenue for each order
        sales_data['total_revenue'] = sales_data['quantity'] * sales_data['price']

        # 3. Group the data by product and calculate the total quantity and revenue
        product_stats = sales_data.groupby('product').agg({
            'quantity': 'sum',
            'total_revenue': 'sum'
        }).reset_index()

        output_file = 'dags/target_data/transformed_sales_data.csv'
        sales_data.to_csv(output_file, index=False)
    except Exception as e:
        raise ValueError(f"Error transforming sales data: {e}")


# Create tasks using PythonOperator
processing_task = PythonOperator(
    task_id='collect_data',
    python_callable=transform_sales_data,
    dag=dag,
)

# Define task dependencies
processing_task
