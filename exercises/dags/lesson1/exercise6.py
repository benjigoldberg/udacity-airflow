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
    redshift_hook.run(sql.COPY_SQL.format(credentials.access_key, credentials.secret_key))


dag = DAG(
    'lesson1.exercise6',
    start_date=datetime.datetime.now()
)

# TODO: What connection should be used?
# TODO: What SQL should be provided?
#create_table = PostgresOperator(
#    task_id="create_table",
#    dag=dag,
#    postgres_conn_id='<REPLACE>'
#    sql=<REPLACE>
#)

# TODO: What Python callable should be provided below?
#copy_task = PythonOperator(
#    task_id='load_from_s3_to_redshift',
#    dag=dag,
#    python_callable=<REPLACE>
#)

# TODO: What connection should be used?
# TODO: What SQL should be provided?
#location_traffic_task = PostgresOperator(
#    task_id="calculate_location_traffic",
#    dag=dag,
#    postgres_conn_id=<REPLACE>,
#    sql=<REPLACE>
#)

# TODO: Define task proper task ordering
# create_table >> <REPLACE>
