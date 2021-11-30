with source as (

    select * from {{ ref('vendor_tags_ss') }} where not _fivetran_deleted

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

    from source

)

select * from renamed

