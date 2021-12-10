{% macro copy_table(src, dst) %}

    {% set sql="create table {} clone {}".format(src, dst) %}
    {% do run_query(sql) %}
    {{ log("table cloned from {} to {}".format(src, dst), info=True) }}

{% endmacro %}
