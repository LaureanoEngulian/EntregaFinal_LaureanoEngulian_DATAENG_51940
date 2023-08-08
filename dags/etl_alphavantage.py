from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

CREATE_TABLE_SQL =  """ 
                        CREATE TABLE IF NOT EXISTS laureanoengulian_coderhouse.big_five_weekly(
                        "week_from" varchar(256) not null,
                        "open" float not null,
                        "high" varchar(256) not null,
                        "low" varchar(256) not null,
                        "close" float not null,
                        "volume" varchar(256) not null,
                        "symbol" varchar(256) not null,
                        "avg" float not null,
                        "pk" varchar(256))
                        distkey (pk)
                        sortkey(pk); 
                    """

CLEAN_DUPLICATES_SQL =  """
                            delete from laureanoengulian_coderhouse.big_five_weekly using laureanoengulian_coderhouse.stage 
                            where big_five_weekly.pk = stage.pk;
                        """

INSERT_DATA_SQL =   """
                        insert into laureanoengulian_coderhouse.big_five_weekly
                        select CAST("week_from" as varchar(256)) as "week_from", 
                        CAST("open" as float) as "open",
                        CAST("high" as varchar(256)) as "high",
                        CAST("low" as varchar(256)) as "low",
                        CAST("close" as float) as "close",
                        CAST("volume" as varchar(256)) as "volume",
                        CAST("symbol" as varchar(256)) as "volume",
                        CAST("avg" as float) as "avg",
                        CAST("pk" as varchar(256)) as "pk"  
                        from laureanoengulian_coderhouse.stage;
                    """

DROP_TEMP_SQL = """ drop table if exists laureanoengulian_coderhouse.stage; """


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

create_table = SQLExecuteQueryOperator(
    task_id="create_table",
    conn_id="redshift_default",
    sql=CREATE_TABLE_SQL,
    dag=dag,
)

etl_alphavantage = SparkSubmitOperator(
    task_id="etl_alphavantage",
    application="/opt/airflow/scripts/ETL_AlphaVantage.py",
    conn_id="spark_default",
    dag=dag,
    driver_class_path=Variable.get("driver_class_path"),
)

clean_sql_alphavantage = SQLExecuteQueryOperator(
    task_id="clean_sql_alphavantage",
    conn_id="redshift_default",
    sql=CLEAN_DUPLICATES_SQL,
    dag=dag,
)

insert_sql_alphavantage = SQLExecuteQueryOperator(
    task_id="insert_sql_alphavantage",
    conn_id="redshift_default",
    sql=INSERT_DATA_SQL,
    dag=dag,
)

drop__temp_sql_alphavantage = SQLExecuteQueryOperator(
    task_id="drop__temp_sql_alphavantage",
    conn_id="redshift_default",
    sql=DROP_TEMP_SQL,
    dag=dag,
)

# Define the sequence of tasks
(
    create_table
    >> etl_alphavantage
    >> clean_sql_alphavantage
    >> insert_sql_alphavantage
    >> drop__temp_sql_alphavantage
)
