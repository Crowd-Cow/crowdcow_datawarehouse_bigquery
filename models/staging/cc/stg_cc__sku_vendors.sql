with source as (

    select * from {{ source('cc', 'sku_vendors') }} where not _fivetran_deleted

),

renamed as (

    select
        id as sku_vendor_id
        ,{{ clean_strings('name') }} as sku_vendor_name
        ,erp_id
        ,launched_at as launched_at_utc
        ,created_at as created_at_utc
        ,updated_at as updated_at_utc
        ,barcode_prefix
        ,sku_plan_id
        ,preferred_fc_id
        ,boxed_beef as is_boxed_beef
        ,marketplace as is_marketplace

    from source

)

select * from renamed

