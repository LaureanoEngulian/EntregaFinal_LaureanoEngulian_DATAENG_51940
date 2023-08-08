from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator



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

etl_alphavantage = SparkSubmitOperator(
        task_id = "etl_alphavantage",
        application = '/opt/airflow/scripts/ETL_AlphaVantage.py',
        conn_id = "spark_default",
        dag = dag,
        driver_class_path = Variable.get("driver_class_path"),
    )


# Define the sequence of tasks
etl_alphavantage 