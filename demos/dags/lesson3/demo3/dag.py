import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.udacity_plugin import HasRowsOperator
from airflow.operators.udacity_plugin import S3ToRedshiftOperator

from lesson3.demo3.subdag import get_s3_to_redshift_dag
import sql


start_date = datetime.datetime.utcnow()

dag = DAG(
    "lesson3.demo3",
    start_date=start_date,
)

trips_task_id = "trips_subdag"
trips_subdag_task = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        "lesson3.demo3",
        trips_task_id,
        "redshift",
        "aws_credentials",
        "trips",
        sql.CREATE_TRIPS_TABLE_SQL,
        s3_bucket="udac-data-pipelines",
        s3_key="divvy/unpartitioned/divvy_trips_2018.csv",
        start_date=start_date,
    ),
    task_id=trips_task_id,
    dag=dag,
)

stations_task_id = "stations_subdag"
stations_subdag_task = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        "lesson3.demo3",
        stations_task_id,
        "redshift",
        "aws_credentials",
        "stations",
        sql.CREATE_STATIONS_TABLE_SQL,
        s3_bucket="udac-data-pipelines",
        s3_key="divvy/unpartitioned/divvy_stations_2017.csv",
        start_date=start_date,
    ),
    task_id=stations_task_id,
    dag=dag,
)

#
# TODO: Migrate Create and Copy tasks to the Subdag
#
create_trips_table = PostgresOperator(
    task_id="create_trips_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql.CREATE_TRIPS_TABLE_SQL
)

copy_trips_task = S3ToRedshiftOperator(
    task_id="load_trips_from_s3_to_redshift",
    dag=dag,
    table="trips",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udac-data-pipelines",
    s3_key="divvy/unpartitioned/divvy_trips_2018.csv"
)

check_trips = HasRowsOperator(
    task_id="check_trips_data",
    dag=dag,
    redshift_conn_id="redshift",
    table="trips"
)

create_stations_table = PostgresOperator(
    task_id="create_stations_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql.CREATE_STATIONS_TABLE_SQL,
)

copy_stations_task = S3ToRedshiftOperator(
    task_id="load_stations_from_s3_to_redshift",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udac-data-pipelines",
    s3_key="divvy/unpartitioned/divvy_stations_2017.csv",
    table="stations"
)

check_stations = HasRowsOperator(
    task_id="check_stations_data",
    dag=dag,
    redshift_conn_id="redshift",
    table="stations"
)

location_traffic_task = PostgresOperator(
    task_id="calculate_location_traffic",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql.LOCATION_TRAFFIC_SQL
)

#
# TODO: Once you have updated the Subdags, reorder the graph
#
create_trips_table >> copy_trips_task
create_stations_table >> copy_stations_task
copy_stations_task >> check_stations
copy_trips_task  >> check_trips
check_stations >> location_traffic_task
check_trips >> location_traffic_task
