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
        
        /*** Busniess decision made to define a "marketplace" SKU as any owner that is NOT Crowd Cow (ID = 91) instead of using the marketplace flag in this table ***/
        --,marketplace as is_marketplace
        ,id <> 91 as is_marketplace

    from source

)

select * from renamed

