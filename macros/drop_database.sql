{% macro drop_database(db) %}

    {% set sql="drop database {}".format(db) %}
    {% do run_query(sql) %}
    {{ log("{} database dropped".format(db), info=True) }}

{% endmacro %}
