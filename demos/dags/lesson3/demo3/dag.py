import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator

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
        "lesson3.exercise3",
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
        "lesson3.exercise3",
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
# TODO: Once you have incorporated this operator into the SubDag
#       Delete `check_trips` and `check_stations`
#
check_trips = HasRowsOperator(
    task_id="check_trips_data",
    dag=dag,
    redshift_conn_id="redshift",
    table="trips"
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
# TODO: Once you have removed the checks from above, reorder the tasks appropriately
#
trips_subdag_task >> check_stations
stations_subdag_task >> check_trips
check_stations >> location_traffic_task
check_trips >> location_traffic_task
