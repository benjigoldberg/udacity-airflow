import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.udacity_plugin import HasRowsOperator
from airflow.operators.udacity_plugin import S3ToRedshiftOperator

import sql


# Returns a DAG which creates a table if it does not exist, and then proceeds
# to load data into that table from S3. When the load is complete, a data
# quality  check is performed to assert that at least one row of data is
# present.
def get_s3_to_redshift_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        aws_credentials_id,
        table,
        create_sql_stmt,
        s3_bucket,
        s3_key,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    #
    # TODO: Move the create table code here
    #

    #
    # TODO: Move the S3ToRedshiftOperator code here
    #

    return dag
