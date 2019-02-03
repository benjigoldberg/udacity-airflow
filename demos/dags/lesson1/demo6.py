import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import sql


def load_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    sql_stmt = sql.COPY_STATIONS_SQL.format(credentials.access_key, credentials.secret_key)
    redshift_hook.run(sql_stmt)


dag = DAG(
    'lesson1.demo6',
    start_date=datetime.datetime.now()
)

# TODO: Give this operator a task ID
# TODO: Assign this operator to the DAG
# TODO: What connection should be used?
#create_table = PostgresOperator(
#    postgres_conn_id='<REPLACE>',
#    sql=sql.CREATE_STATIONS_TABLE_SQL
#)

# TODO: Give this operator a task ID
# TODO: Assign this operator to the DAG
# TODO: Populate the Python Callable Below
#copy_task = PythonOperator()


# TODO: Define task proper task ordering
# create_table >> <REPLACE>
