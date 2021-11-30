with source as (

    select * from {{ source('cc', 'bid_item_sku_with_quantities') }} where not _fivetran_deleted

),

renamed as (

    select
        id as bid_item_sku_quantity_id
        ,dbt_scd_id as bid_item_sku_quantity_key
        ,sku_id
        ,updated_at as updated_at_utc
        ,created_at as created_at_utc
        ,quantity as sku_quantity
        ,bid_item_id

    from source

)

select * from renamed

