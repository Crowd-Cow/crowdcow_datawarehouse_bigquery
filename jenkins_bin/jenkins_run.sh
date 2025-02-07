#!/bin/bash

# exit when any command fails
set -e

dbt seed --target qa 
dbt snapshot 
dbt run --target qa 
dbt test --target qa
dbt clone --select models/ --state target/ --target prod --target-path target_clone --full-refresh
dbt clone --select models/ --state target/ --target dev --target-path target_clone --full-refresh
