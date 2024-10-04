{% macro convert_percent(column_name, precision=2) %}
  CASE
    WHEN {{ column_name }} > 1 THEN ROUND(CAST({{ column_name }} AS FLOAT64) / 100, {{ precision }})
    ELSE {{ column_name }}
  END
{% endmacro %}
