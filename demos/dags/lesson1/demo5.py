import datetime
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook


def log_details(*args, **kwargs):
    #
    # TODO: Log details of this run that were passed in by the Airflow context
    #
    #       Look here for context variables passed in on kwargs:
    #       https://airflow.apache.org/code.html#macros
    pass

dag = DAG(
    'lesson1.demo5',
    schedule_interval="@daily",
    start_date=datetime.datetime.now() - datetime.timedelta(days=2)
)

# TODO: Provide Context
list_task = PythonOperator(
    task_id="log_details",
    python_callable=log_details,
    dag=dag
)
