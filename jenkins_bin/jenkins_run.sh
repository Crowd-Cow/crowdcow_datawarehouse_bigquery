#!/bin/bash

# exit when any command fails
set -e

dbt seed
dbt snapshot
dbt run
dbt test
dbt run-operation swap_database
