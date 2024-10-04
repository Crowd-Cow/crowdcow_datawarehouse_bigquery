with

source as ( select * from {{ source('shipwell', 'purchase_orders') }} )

,renamed as (
    select 
        id as purchase_order_id
        ,line_item.value:id::text as line_item_id
        ,line_item.value:package_type::text as package_type
        ,line_item.value:package_weight::float as item_package_weight
        ,line_item.value:total_packages::int as item_total_packages
        ,line_item.value:total_line_item_weight::float as item_total_weight
        ,line_item.value:created_at::timestamp as item_created_at_utc
        ,line_item.value:updated_at::timestamp as item_updated_at_utc
    from source,
        lateral flatten ( input => line_items ) line_item
)

select * from renamed
