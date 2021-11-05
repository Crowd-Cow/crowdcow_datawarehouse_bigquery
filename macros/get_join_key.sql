{%- macro get_join_key(key_model, key_name, key_id_lookup, id_table, id_lookup, id_valid_date) -%}

    {%- set key_table_ref = builtins.ref(key_model).include(database=false) -%}
    
    (
        select max( {{ key_name }} ) as key 
            from {{ key_table_ref }}
            where {{ key_id_lookup }} = {{ id_table }}.{{ id_lookup }} 
                and {{ id_table }}.{{ id_valid_date }} between adjusted_dbt_valid_from and adjusted_dbt_valid_to
    )
  
{%- endmacro -%}
