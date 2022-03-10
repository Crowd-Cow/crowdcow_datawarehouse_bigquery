{%- macro generate_tag(ref, id_field, tag_key, tag_purpose) -%}

{%- set table_ref = builtins.ref(ref).include(database=false) -%}

    select
        {{ dbt_utils.surrogate_key(['{}'.format(id_field),"'{}'".format(tag_key)]) }} as tag_id
        ,'{{ table_ref }}' as tag_source_table
        ,{{ id_field }}
        ,'{{ tag_key }}' as tag_key
        ,'{{ tag_purpose }}' as tag_purpose
        ,sysdate() as created_at_utc
        ,sysdate() as updated_at_utc
    from {{ table_ref }}
  
{%- endmacro -%}