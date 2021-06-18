{% macro cents_to_dollars(column_name, precision=2) %}
  (round({{ column_name }}::float / 100.0, {{ precision }}))
{% endmacro %}
