{% macro swap_database() %}

    {% set sql='alter database analytics swap with analytics_qa' %}
    {% do run_query(sql) %}
    {{ log("database swapped", info=True) }}

{% endmacro %}
