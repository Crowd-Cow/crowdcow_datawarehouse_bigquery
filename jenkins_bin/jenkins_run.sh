#!/bin/bash

# exit when any command fails
set -e

dbt run-operation clone_database --args '{db_name: analytics_qa, db_to_clone: analytics}'
dbt seed
dbt snapshot
dbt run
dbt test
dbt run-operation swap_database --args '{db: analytics, swap_with: analytics_qa}'
dbt run-operation grant_permission_on_database --args '{permission_type: usage, db: analytics, role: reporter}'
dbt run-operation grant_permission_on_database --args '{permission_type: usage, db: analytics, role: fountain9}'
dbt run-operation grant_permission_on_database --args '{permission_type: usage, db: analytics, role: ampush}'
dbt run-operation drop_database --args '{db: analytics_qa}'
