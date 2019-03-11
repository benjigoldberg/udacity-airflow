from airflow import DAG


#
# TODO: Create a DAG which performs the following functions:
#
#       1. Loads Trip data from S3 to RedShift
#       2. Performs a data quality check on the Trips table in RedShift
#       3. Uses the FactsCalculatorOperator to create a Facts table in Redshift based on
#          `tripduration` of the bikes.
#           a. **NOTE**: to complete this step you must complete the FactsCalcuatorOperator
#              skeleton defined in plugins/operators/facts_calculator.py
#
dag = DAG("lesson3.exercise4")

#
# TODO: Load trips data from S3 to RedShift
#

#
# TODO: Perform a data quality check on the Trips table
#

#
# TODO: Use the FactsCalculatorOperator to create a Facts table in RedShift
#

#
# TODO: Define task ordering for the DAG tasks you defined
#
