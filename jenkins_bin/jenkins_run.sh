#!/bin/bash

# exit when any command fails
set -e

echo "Current directory:"
pwd

echo "Listing all files in /tmp:"
ls -la /tmp/

echo "Listing /tmp/service-account-key.jsonfile inside the container:"
ls -l /tmp/service-account-key.jsonfile
file /tmp/service-account-key.jsonfile

dbt seed --target qa 
dbt snapshot 
dbt run --target qa 
dbt test --target qa
dbt run --target prod  
dbt test --target qa