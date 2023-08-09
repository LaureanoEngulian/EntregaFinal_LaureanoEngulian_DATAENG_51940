import smtplib
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

CREATE_TABLE_SQL = """ 
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

CLEAN_DUPLICATES_SQL = """
                            delete from laureanoengulian_coderhouse.big_five_weekly using laureanoengulian_coderhouse.stage 
                            where big_five_weekly.pk = stage.pk;
                        """

INSERT_DATA_SQL = """
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

UNIQUE_SQL = """ select (count(pk)/count(distinct pk))as unq from laureanoengulian_coderhouse.big_five_weekly; """


def send_email(**kwargs):
    result_value = kwargs["ti"].xcom_pull(task_ids="chk_unique_sql")
    sender_email = Variable.get("sender_email")
    sender_password = Variable.get("sender_password")
    recipient_email = Variable.get("recipient_email")

    subject = "Resultado del proceso"
    if result_value is not None:
        if result_value[0][0] == 1:
            message = "Proceso exitoso. No hay duplicados."
        else:
            message = "Error en el proceso. Se generaron duplicados."
    else:
        message = "Error en el proceso general. Revisar el DAG"

    email_text = f"Subject: {subject}\n\n{message}"
    print(email_text)

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, recipient_email, email_text)


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

drop_temp_sql_alphavantage = SQLExecuteQueryOperator(
    task_id="drop_temp_sql_alphavantage",
    conn_id="redshift_default",
    sql=DROP_TEMP_SQL,
    dag=dag,
)

chk_unique_sql = SQLExecuteQueryOperator(
    task_id="chk_unique_sql",
    conn_id="redshift_default",
    sql=UNIQUE_SQL,
    dag=dag,
    do_xcom_push=True,
)

send_email_task = PythonOperator(
    task_id="send_email",
    python_callable=send_email,
    provide_context=True,
    dag=dag,
)

# Define the sequence of tasks
(
    create_table
    >> etl_alphavantage
    >> clean_sql_alphavantage
    >> insert_sql_alphavantage
    >> drop_temp_sql_alphavantage
    >> chk_unique_sql
    >> send_email_task
)
