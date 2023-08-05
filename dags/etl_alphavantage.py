# Este es el DAG que orquesta el ETL de la tabla users

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

defaul_args = {
    "owner": "Laureano Engulian",
    "start_date": datetime(2023, 7, 23),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="etl_alphavantage",
    default_args=defaul_args,
    description="ETL de la tabla big_five_weekly",
    schedule_interval="@daily",
    catchup=False,
) as dag:

    etl = BashOperator(
        task_id="execute_python",
        bash_command="python3 /opt/airflow/scripts/ETL_AlphaVantage.py",
    )

    etl
