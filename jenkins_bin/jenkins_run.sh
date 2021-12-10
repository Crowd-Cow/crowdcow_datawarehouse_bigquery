#!/bin/bash

# exit when any command fails
set -e

dbt run-operation clone_database --args '{db_name: analytics_qa, db_to_clone: analytics}'
dbt seed
dbt snapshot
dbt run
dbt test
dbt run-operation swap_database --args '{db: analytics, swap_with: analtyics_qa}'
dbt run-operation drop_database --args '{db: analytics_qa}'
