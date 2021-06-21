{% macro clean_strings(column_name) %}

    case
        when {{ column_name }} = '' then null
        else upper(trim( {{ column_name }} ))
    end

{% endmacro %}
