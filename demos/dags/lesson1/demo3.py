import datetime
import logging
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def hello_world():
    logging.info("Hello World")


def current_time():
    logging.info(f"Current time is {datetime.datetime.utcnow().isoformat()}")


def working_dir():
    logging.info(f"Working directory is {os.getcwd()}")


def complete():
    logging.info("Congrats, your first multi-task pipeline is now complete!")


dag = DAG(
    "lesson1.demo3",
    schedule_interval='@hourly',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1))

hello_world_task = PythonOperator(
    task_id="hello_world",
    python_callable=hello_world,
    dag=dag)

# TODO: Create Tasks and Operators


#
# TODO: Configure the task dependencies such that the graph looks like the following:
#
#                    -> current_time_task
#                   /                    \
#   hello_world_task                      -> division_task
#                   \                    /
#                    -> working_dir_task
