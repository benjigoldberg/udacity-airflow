import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators import (
    HasRowsOperator,
    PostgresOperator,
    PythonOperator,
    S3ToRedshiftOperator
)

import sql


def load_and_analyze(*args, **kwargs):
    # TODO: too many things
    pass


dag = DAG(
    "lesson3.exercise2",
    start_date=datetime.datetime(2018, 1, 1, 0, 0, 0, 0),
)

load_and_analyze = PythonOperator(
    task_id='load_and_analyze',
    dag=dag,
    python_callable=load_and_analyze,
    provide_context=True,
)

