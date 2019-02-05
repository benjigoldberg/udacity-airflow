# udac-airflow

This repository contains the exercises and solutions for the Udacity Data Pipelines course.

## Prerequisites
Before running these exercises you will need to [install Docker](https://www.docker.com/get-started)

## Directory Layout

* [`demos`](demos) contains the instructor demonstrations.
* [`exercises`](exercises) contains the student exercises.
* [`solutions`](solutions) contains the full exercise solutions

Each directory contains a `docker-compose.yml` file.

## Running Airflow

To run Airflow in Docker Compose: `docker-compose up`
To stop Airflow in Docker Compose: `docker-compose down`

Once you are running the Docker Compose Airflow, the DAGs and plugins in that directory will
automatically be loaded by Airflow.

Navigate to the Airflow UI by opening a browser to [`http://localhost:8080`](http://localhost:8080)
