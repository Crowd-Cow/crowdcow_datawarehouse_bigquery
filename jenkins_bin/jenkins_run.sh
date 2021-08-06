#!/bin/bash

# exit when any command fails
set -e

source ~/.profile

export SNOWSQL_DEST=~/bin 
export SNOWSQL_LOGIN_SHELL=~/.profile
export SNOWSQL_ACCOUNT=lna65058.us-east-1
export SNOWSQL_USER=$SNOWFLAKE_DATAWAREHOUSE_USER
export SNOWSQL_PWD=$SNOWFLAKE_DATAWAREHOUSE_PASSWORD

echo "Creating a QA database"
snowsql -f /usr/src/app/jenkins_bin/create_qa_databases.sql
echo "Running dbt"
dbt seed --target=qa
dbt snapshot --target=qa
dbt run --target=qa
dbt test --target=qa
echo "Swapping the QA database with Prod"
snowsql -f /usr/src/app/jenkins_bin/swap_databases.sql
echo "Dropping the QA database"
snowsql -f /usr/src/app/jenkins_bin/drop_qa_databases.sql
