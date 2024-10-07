#!/bin/bash

# exit when any command fails
set -e

echo "Current directory:"
pwd

echo "Listing all files in /tmp:"
ls -la /tmp/

echo "Listing /tmp/service-account-key.json inside the container:"
ls -l /tmp/service-account-key.json
file /tmp/service-account-key.json

dbt seed --target qa 
dbt snapshot 
dbt run --target qa 
dbt test --target qa
dbt run --target prod  
dbt test --target qa