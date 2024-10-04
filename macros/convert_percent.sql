{% macro convert_percent(column_name, precision=2) %}
  case
    when {{ column_name }} > 1 then (round({{ column_name }}::float / 100.0, {{ precision }}))
    else {{ column_name }}
  end
{% endmacro %}
