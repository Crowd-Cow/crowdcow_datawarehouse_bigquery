{% macro grant_permission_on_schema(permission_type, db, role) %}

    {% set sql="grant {} on schema {} to role {}".format(permission_type, db, role) %}
    {% do run_query(sql) %}
    {{ log("permission granted on {} to {}".format(db, role), info=True) }}

{% endmacro %}
