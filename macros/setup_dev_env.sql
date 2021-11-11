{% macro setup_dev_env(clone_db,action) %}

{# This macro to sets up a development environment for a user by cloning a given database
Based on the given action, it will create or destroy a development database based on the user 
running the macro and what target environment they are using. 

For example:
  - User `user1` is using the default dbt target of `dev` and clones the proddb database
    - The database that is created or destroyed: user_db = `user1_proddb_dev`
  - User `user2` is using the target flag of --target qa and clones the proddb database
    - The database that is created or destroyed: user_db = `user2_proddb_qa` 
    
Using the `dry_run` action will output the sql statements that would run given the create or destroy action
This can be useful to make sure the clone/drop table statement will do what is intended 

Command to run the macro from the command line: dbt run-operation setup_dev_env --args '{ "clone_db": "<db_name>", "action":"<create/destroy/dry_run>"}' #}

{% set user_db = target.user + '_' + clone_db + '_' + target.name %}
{% set clone_sql = 'create or replace database ' + user_db + ' clone ' + clone_db %}
{% set transfer_ownership_sql = 'grant ownership on database ' + user_db + ' to role transformer' %}
{% set drop_sql = 'drop database if exists ' + user_db %}

{% if action == 'create' %}

    {{ log("Cloning Database: " + clone_db + " -> " + user_db, info=True) }}
    {{ log("Running SQL on Snowflake: " + clone_sql, info=True) }}

    {% do run_query(clone_sql) %}

    {{ log("Database " + user_db + " cloned", info=True) }}
    {{ log("Transferring ownership: " + transfer_ownership_sql, info=True) }}

    {% do run_query(transfer_ownership_sql) %}

    {{ log("Ownership transferred", info=True) }}

{% elif action == 'destroy' %}
    
    {{ log("Dropping DB: " + user_db, info=True) }}
    {{ log("Running SQL on Snowflake: " + drop_sql, info=True) }}

    {% do run_query(drop_sql) %}

    {{ log("Database dropped", info=True) }}

{% elif action == 'dry_run' %}

    {{ log("action = create will run the following sql statment:", info=True) }}
    {{ log("    " + clone_sql, info=True) }}
    {{ log(" ", info=True) }}
    {{ log("action = destroy will run the following sql statement:", info=True) }}
    {{ log("    " + drop_sql, info=True) }}

{% else %}

    {{ log("Selected action is not supported. Please set action as `create`, `destroy`, or `dry_run`.", info=True) }}

{% endif %}
  
{% endmacro %}
