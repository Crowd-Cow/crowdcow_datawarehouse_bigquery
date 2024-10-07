#!/bin/bash

# exit when any command fails
set -e

export KEYFILE_PATH=/tmp/service-account-key.json

# Create the directory for the keyfile, not the keyfile path itself
mkdir -p $(dirname "$KEYFILE_PATH")

echo "Contents of /tmp before dbt run:"
ls -l /tmp

# Verify the keyfile inside the container
echo "Checking keyfile at $KEYFILE_PATH"
ls -l "$KEYFILE_PATH"
file "$KEYFILE_PATH"


dbt seed --target qa 
dbt snapshot 
dbt run --target qa 
dbt test --target qa
dbt run --target prod  
dbt test --target qa