#!/bin/bash

# exit when any command fails
set -e

dbt seed --target qa 
dbt snapshot 
dbt run --target qa 
dbt test --target qa
dbt run --target prod  
dbt test --target qa