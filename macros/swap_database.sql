{% macro swap_database(db, swap_with) %}

    {% set sql="alter database {} swap with {}".format(db, swap_with) %}
    {% do run_query(sql) %}
    {{ log("{} database swapped with {}".format(db, swap_with), info=True) }}

{% endmacro %}
