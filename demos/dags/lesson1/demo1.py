import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


#
# TODO: Define a function for the PythonOperator to call
#
def greet():
    logging.info("Hello World")


dag = DAG(
        'lesson1.demo1',
        start_date=datetime.datetime.utcnow())

greet_task = PythonOperator(
    task_id="greet_task",
    python_callable=greet,
    dag=dag
)
