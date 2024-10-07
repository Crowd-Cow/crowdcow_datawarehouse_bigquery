#!/bin/bash

# exit when any command fails
set -e

echo "Listing service_account.json inside the container:"
ls -l /usr/src/app/service_account.json

dbt seed --target qa 
dbt snapshot 
dbt run --target qa 
dbt test --target qa
dbt run --target prod  
dbt test --target qa