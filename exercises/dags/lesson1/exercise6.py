import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator


CREATE_TABLE_SQL = """
DROP TABLE IF EXISTS trips;
CREATE TABLE trips (
trip_id INTEGER NOT NULL,
start_time TIMESTAMP NOT NULL,
end_time TIMESTAMP NOT NULL,
bikeid INTEGER NOT NULL,
tripduration DECIMAL(16,2) NOT NULL,
from_station_id INTEGER NOT NULL,
from_station_name VARCHAR(100) NOT NULL,
to_station_id INTEGER NOT NULL,
to_station_name VARCHAR(100) NOT NULL,
usertype VARCHAR(20),
gender VARCHAR(6),
birthyear INTEGER,
PRIMARY KEY(trip_id))
DISTSTYLE ALL;
"""

COPY_SQL = """
COPY trips
FROM 's3://udac-data-pipelines/divvy/unpartitioned/divvy_2018.csv'
ACCESS_KEY_ID '{}'
SECRET_ACCESS_KEY '{}'
IGNOREHEADER 1
DELIMITER ','
"""

LOCATION_TRAFFIC_SQL = """
DROP TABLE IF EXISTS station_traffic;
CREATE TABLE station_traffic AS
SELECT
    DISTINCT(t.from_station_id) AS station_id,
    t.from_station_name AS station_name,
    num_departures,
    num_arrivals
FROM trips t
JOIN (
    SELECT
        from_station_id,
        COUNT(from_station_id) AS num_departures
    FROM trips
    GROUP BY from_station_id
) AS fs ON t.from_station_id = fs.from_station_id
JOIN (
    SELECT
        to_station_id,
        COUNT(to_station_id) AS num_arrivals
    FROM trips
    GROUP BY to_station_id
) AS ts ON t.from_station_id = ts.to_station_id
"""


def load_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    redshift_hook.run(COPY_SQL.format(credentials.access_key, credentials.secret_key))


dag = DAG(
    'exercise6',
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
