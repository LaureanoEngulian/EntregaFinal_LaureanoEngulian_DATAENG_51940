import sys
from datetime import datetime, timedelta
from os import environ as env

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from ETL_AlphaVantage import AlphaVantageETL

sys.path.append("/opt/airflow/scripts")

# Define DAG arguments
default_args = {
    "owner": "Laureano Engulian",
    "start_date": datetime(2023, 8, 6),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Create a DAG instance
dag = DAG(
    "alpha_vantage_etl_dag",
    default_args=default_args,
    description="DAG for AlphaVantage ETL process - big_five_weekly table",
    schedule_interval="@daily",
    catchup=False,
)

# Instance of the AlphaVantageETL class
etl_instance = AlphaVantageETL()

symbol_list = ["GOOG", "AMZN", "METV", "AAPL", "MSFT"]


# Define tasks as Python functions
def combine_data_task():
    etl_instance.combine_data(symbol_list=symbol_list, api_key=env["API_KEY"])


def transform_task():
    df_combined = etl_instance.combine_data(
        symbol_list=Variable.get("symbol_list"), api_key=env["API_KEY"]
    )
    etl_instance.transform(df_combined)


# Create operations using the PythonOperator
combine_data_operator = PythonOperator(
    task_id="combine_data_task",
    python_callable=combine_data_task,
    dag=dag,
)

transform_operator = PythonOperator(
    task_id="transform_task",
    python_callable=transform_task,
    dag=dag,
)

# Define the sequence of tasks
combine_data_operator >> transform_operator
