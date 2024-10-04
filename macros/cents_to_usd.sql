{% macro cents_to_usd(column_name, precision=2) %}
  ROUND(CAST({{ column_name }} AS FLOAT64) / 100, {{ precision }})
{% endmacro %}
