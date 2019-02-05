# Solutions

Solutions can be found in [`dags`](dags) and [`plugins`](plugins).

Solutions are organized by lesson and exercise within each folder.

## Running the solutions

You can run the solutions on your own machine using `docker-compose`. To use `docker-compose`, you
must first [install Docker](https://docs.docker.com/install/). Once Docker is installed:

1. Open a terminal in the same directory as [`docker-compose.yml`](docker-compose.yml) an
1. Run `docker-compose up`
1. Wait 30-60 seconds
1. Open [`http://localhost:8080`](http://localhost:8080) in Google Chrome (Other browsers occasionally have issues rendering the Airflow UI)
1. Make sure you have configured the `aws_credentials` and `redshift` credentials for all lessons after lesson 1, exercise 6

When you are ready to quit Airflow, hit `ctrl+c` in the terminal where `docker-compose` is running.
Then, type `docker-compose down`
