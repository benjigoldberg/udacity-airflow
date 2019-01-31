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
