{% macro clone_schema(src, dst) %}

    {% set sql="create schema {} clone {}".format(src, dst) %}
    {% do run_query(sql) %}
    {{ log("schema cloned from {} to {}".format(src, dst), info=True) }}

{% endmacro %}
