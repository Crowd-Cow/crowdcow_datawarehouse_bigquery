-- Generate columns that are all cast as strings, from a list of column names.
-- Because we can't control the data type that Fivetran infers for each column we are creating two views on the raw data.
-- The first casts every column as a string and the second view casts each column to the desired data type.
-- This macro simplifies the repetitive casting of all columns to string.

{% macro google_sheets_stg_strings(column_names) -%}

  {% for col in column_names %}
    upper(nullif(trim({{ col }}::text), '')) as {{ col }}{{ "," if not loop.last }}
  {% endfor %}

{%- endmacro %}
