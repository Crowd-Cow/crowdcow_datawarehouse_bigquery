with

packed_sku as ( select * from {{ ref('int_packed_skus') }} )
,vendor as ( select * from {{ ref('stg_cc__sku_vendors') }} )
,sku as ( select * from {{ ref('skus') }} )
,order_type_details as (select order_id, order_type from {{ ref('stg_cc__orders') }})

,get_qvc_skus as (
    select
        packed_sku.order_item_detail_id
        ,packed_sku.order_id
        ,case when order_type_details.order_type = 'QVC' then true else false end as is_qvc
        ,packed_sku.sku_id
        ,packed_sku.sku_key
        ,packed_sku.sku_box_id
        ,packed_sku.sku_box_key
        ,packed_sku.sku_owner_id
        ,packed_sku.lot_number
        ,packed_sku.fc_id
        ,packed_sku.fc_key
        ,vendor.sku_vendor_name as owner_name
        ,packed_sku.sku_quantity
        ,packed_sku.created_at_utc
        ,packed_sku.updated_at_utc
        ,packed_sku.packed_created_at_utc
    from packed_sku 
        left join vendor on packed_sku.sku_owner_id = vendor.sku_vendor_id
        left join order_type_details on packed_sku.order_id = order_type_details.order_id
)

,get_sku_details as (
    select
        get_qvc_skus.*
        ,get_qvc_skus.sku_quantity * sku.sku_weight as total_sku_weight
    from get_qvc_skus
        left join sku on get_qvc_skus.sku_key = sku.sku_key
)

select * from get_sku_details where is_qvc
