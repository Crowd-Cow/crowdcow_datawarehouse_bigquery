{%- macro restore_schema_final(schema_name, restore_timestamp, start_after_table=None) -%}
  
  {#- Macro setup: Initialize lists and the namespace for state management -#}
  {%- set successful_tables = [] -%}
  {%- set failed_tables = [] -%}
  {%- set ns = namespace(can_process = not start_after_table) -%}

  {{ log("🚀 Starting definitive restore for schema '" ~ schema_name ~ "'", info=True) }}
  {%- if start_after_table -%}
    {{ log("▶️ Resuming run, will start processing AFTER table: " ~ start_after_table, info=True) }}
  {%- endif -%}

  {%- set schema_relation = api.Relation.create(database=target.project, schema=schema_name) -%}
  {%- set relations = adapter.list_relations_without_caching(schema_relation) -%}

  {#- Loop through every relation (table/view) in the dataset -#}
  {%- for relation in relations -%}
    
    {#- Main control flow to handle restarting and skipping tables -#}
    {%- if ns.can_process -%}
        {#- This is the main processing block for all tables that should be processed. -#}
        {{ log("--- Processing table: " ~ relation ~ " ---", info=True) }}
        
        {#- Step 1: Pre-flight check for creation time to avoid invalid time travel errors -#}
        {%- set ddl_query -%}
          SELECT ddl FROM `{{ relation.database }}.{{ relation.schema }}.INFORMATION_SCHEMA.TABLES`
          WHERE table_name = '{{ relation.identifier }}'
            AND creation_time <= TIMESTAMP('{{ restore_timestamp }}')
        {%- endset -%}
        {%- set ddl_results = run_query(ddl_query) -%}

        {%- if not ddl_results.rows -%}
            {%- set reason = "Skipped: Table was created after the restore timestamp." -%}
            {{ log("⏭️ " ~ reason, info=True) }}
            {%- do failed_tables.append({'table': relation.render(), 'error': reason}) -%}
        {%- else -%}
            {#- If pre-flight checks pass, parse the DDL and attempt the restore -#}
            {%- set ddl_string = ddl_results.columns[0].values()[0] -%}
            {%- set ddl_upper = ddl_string | upper -%}
            {%- set partition_clause, cluster_clause = '', '' -%}

            {#- Safely extract Partitioning clause (case-insensitive) -#}
            {%- if 'PARTITION BY' in ddl_upper -%}
              {%- set p_temp = ddl_upper.split('PARTITION BY')[1] -%}
              {%- set p_expr = p_temp.split('CLUSTER BY')[0].split('OPTIONS(')[0] | trim | replace(';', '') -%}
              {%- if p_expr -%}
                {%- set partition_clause = 'PARTITION BY ' ~ p_expr -%}
              {%- endif -%}
            {%- endif -%}

            {#- Safely extract Clustering clause (case-insensitive) -#}
            {%- if 'CLUSTER BY' in ddl_upper -%}
              {%- set c_temp = ddl_upper.split('CLUSTER BY')[1] -%}
              {%- set c_expr = c_temp.split('OPTIONS(')[0] | trim | replace(';', '') -%}
              {%- if c_expr -%}
                {%- set cluster_clause = 'CLUSTER BY ' ~ c_expr -%}
              {%- endif -%}
            {%- endif -%}

            {#- Build the final SQL statement cleanly -#}
            {%- set restore_sql -%}
              CREATE OR REPLACE TABLE {{ relation }}
              {%- if partition_clause %}
              {{ partition_clause }}
              {%- endif %}
              {%- if cluster_clause %}
              {{ cluster_clause }}
              {%- endif %}
              AS SELECT * FROM {{ relation }}
              FOR SYSTEM_TIME AS OF TIMESTAMP('{{ restore_timestamp }}');
            {%- endset -%}

            {#- Execute the restore and check the response code for success -#}
            {%- call statement('restore_run', fetch_result=False, auto_begin=False) -%}
              {{ restore_sql }}
            {%- endcall -%}
            {%- set response = load_result('restore_run').response -%}

            {%- if response.code == 200 -%}
              {{ log("✔️ Successfully restored " ~ relation, info=True) }}
              {%- do successful_tables.append(relation.render()) -%}
            {%- else -%}
              {{ log("❌ FAILED to restore " ~ relation, info=True) }}
              {%- do failed_tables.append({'table': relation.render(), 'error': response.message}) -%}
            {%- endif -%}
        {%- endif -%}

    {%- elif not ns.can_process and relation.identifier | lower == start_after_table | lower -%}
        {#- This block executes only ONCE when the restart table is found -#}
        {%- set ns.can_process = True -%}
        {{ log("✅ Found restart point '" ~ start_after_table ~ "'. Resuming process with the next table.", info=True) }}
    {%- else -%}
        {#- This block executes for all tables before the restart point is found -#}
        {{ log("⏩ Skipping: " ~ relation.identifier, info=True) }}
    {%- endif -%}

  {%- endfor -%}
  
  {#- Final Summary Report -#}
  {{ log("-----------------------------------------------------------------", info=True) }}
  {{ log("🏁 Restore run complete. Final Summary:", info=True) }}
  {{ log("   Successful: " ~ successful_tables | length, info=True) }}
  {{ log("   Failed/Skipped: " ~ failed_tables | length, info=True) }}
  {%- if failed_tables | length > 0 -%}
    {{ log("--- Details for Failed/Skipped Tables ---", info=True) }}
    {%- for failure in failed_tables -%}
      {{ log("Table: " ~ failure.table, info=True) }}
      {{ log("  Reason: " ~ failure.error, info=True) }}
    {%- endfor -%}
  {%- endif -%}
  {{ log("-----------------------------------------------------------------", info=True) }}

{%- endmacro -%}