{% macro clone_database(db_name, db_to_clone) %}
    {# Get the project ID from the target context #}
    {% set project_id = target.project %}
    
    {# Set the source and target datasets with project ID #}
    {% set source_dataset = "`{}.{}.{}`".format(project_id, db_to_clone, "") %}
    {% set target_dataset = "`{}.{}.{}`".format(project_id, db_name, "") %}
    
    {# Create the target dataset if it doesn't exist #}
    {% set create_dataset_sql = "CREATE SCHEMA IF NOT EXISTS {} ;".format(target_dataset) %}
    {% do run_query(create_dataset_sql) %}
    
    {# Get the list of tables and views from the source dataset #}
    {% set query = """
        SELECT table_name, table_type
        FROM `{}`.INFORMATION_SCHEMA.TABLES
    """.format(source_dataset.strip('`')) %}
    {% set table = run_query(query) %}
    
    {# Loop over the tables and views #}
    {% for row in table.rows %}
        {% set table_name = row['table_name'] %}
        {% set table_type = row['table_type'] %}
    
        {% if table_type == 'VIEW' %}
            {# Get the view definition #}
            {% set view_definition_query = """
                SELECT view_definition
                FROM `{}`.INFORMATION_SCHEMA.VIEWS
                WHERE table_name = '{}'
            """.format(source_dataset.strip('`'), table_name) %}
            {% set view_definition_table = run_query(view_definition_query) %}
            {% set view_definition = view_definition_table.rows[0]['view_definition'] %}
    
            {# Create the view in the target dataset #}
            {% set create_view_sql = """
                CREATE OR REPLACE VIEW `{target_dataset}.{table_name}` AS
                {view_definition}
            """.format(
                target_dataset=target_dataset.strip('`'),
                table_name=table_name,
                view_definition=view_definition
            ) %}
            {% do run_query(create_view_sql) %}
            {{ log("View '{}' created in dataset '{}'".format(table_name, target_dataset), info=True) }}
        {% else %}
            {# Copy the table to the target dataset #}
            {% set source_table = "`{}`".format("{}.{}".format(source_dataset.strip('`'), table_name)) %}
            {% set target_table = "`{}`".format("{}.{}".format(target_dataset.strip('`'), table_name)) %}
            {% set copy_table_sql = """
                CREATE OR REPLACE TABLE {target_table} AS
                SELECT * FROM {source_table}
            """.format(
                target_table=target_table,
                source_table=source_table
            ) %}
            {% do run_query(copy_table_sql) %}
            {{ log("Table '{}' copied to dataset '{}'".format(table_name, target_dataset), info=True) }}
        {% endif %}
    {% endfor %}
    
    {{ log("Dataset '{}' cloned from '{}'".format(target_dataset, source_dataset), info=True) }}
{% endmacro %}