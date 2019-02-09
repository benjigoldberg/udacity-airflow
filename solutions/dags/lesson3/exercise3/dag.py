import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator

from lesson3.exercise3.subdag import get_s3_to_redshift_dag
import sql


start_date = datetime.datetime.utcnow()

dag = DAG(
    "lesson3.exercise3",
    start_date=start_date,
)

trips_task_id = "trips_subdag"
trips_subdag_task = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        "lesson3.exercise3",
        trips_task_id,
        "redshift",
        "aws_credentials",
        "trips",
        sql.CREATE_TRIPS_TABLE_SQL,
        s3_bucket="udacity-dend",
        s3_key="data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv",
        start_date=start_date,
    ),
    task_id=trips_task_id,
    dag=dag,
)

stations_task_id = "stations_subdag"
stations_subdag_task = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        "lesson3.exercise3",
        stations_task_id,
        "redshift",
        "aws_credentials",
        "stations",
        sql.CREATE_STATIONS_TABLE_SQL,
        s3_bucket="udacity-dend",
        s3_key="data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv",
        start_date=start_date,
    ),
    task_id=stations_task_id,
    dag=dag,
)

location_traffic_task = PostgresOperator(
    task_id="calculate_location_traffic",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql.LOCATION_TRAFFIC_SQL
)

trips_subdag_task >> location_traffic_task
stations_subdag_task >> location_traffic_task
