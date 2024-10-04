{% macro process_text(field_name) %}

    -- Replace specific characters or patterns
    REPLACE(
        REGEXP_REPLACE(
            {{ field_name }},
            '[^a-zA-Z0-9\\s]',  -- Regex pattern to remove non-alphanumeric characters
            ' '
        ),
        '  ',  -- Replace double spaces if any
        ' '
    )

{% endmacro %}
