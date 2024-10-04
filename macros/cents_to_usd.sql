{% macro cents_to_usd(column_name, precision=2) %}
  (round({{ column_name }}::float / 100.0, {{ precision }}))
{% endmacro %}
