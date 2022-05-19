{% macro clone_database(db_name, db_to_clone) %}

    {% set sql="create or replace database {} clone {}".format(db_name, db_to_clone) %}
    {% do run_query(sql) %}
    {{ log("{} database cloned from {}".format(db_name, db_to_clone), info=True) }}

{% endmacro %}
