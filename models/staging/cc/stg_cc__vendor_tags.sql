with source as (

    select * from {{ ref('vendor_tags_ss') }} 

),

renamed as (

    select
        id as vendor_tag_id
        ,dbt_scd_id as vendor_tag_dbt_key
        ,{{ clean_strings('key') }} as vendor_tag_key
        ,updated_at as updated_at_utc
        ,{{ clean_strings('value') }} as vendor_tag_value
        ,created_at as created_at_utc
        ,{{ clean_strings('description') }} as vendor_tag_description
        ,dbt_valid_to
        ,dbt_valid_from
        ,case
            when dbt_valid_from = first_value(dbt_valid_from) over(partition by id order by dbt_valid_from) then '1970-01-01'
            else dbt_valid_from
        end as adjusted_dbt_valid_from
        ,coalesce(dbt_valid_to,'2999-01-01') as adjusted_dbt_valid_to

    from source

)

select * from renamed

