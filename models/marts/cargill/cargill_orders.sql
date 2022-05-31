with

order_info as ( select * from {{ ref('orders') }} )
--,order_item_detail as ( select * from {{ ref('order_item_details') }} )
--,sku as ( select * from {{ ref('skus') }} )

/*,cargill_order_items as (
    select distinct
        order_item_detail.order_id
    from order_item_detail
        left join sku on order_item_detail.sku_key = sku.sku_key
    where sku.is_cargill
)

,cargill_orders as (
    select
        order_info.*
    from order_info
        inner join cargill_order_items on order_info.order_id = cargill_order_items.order_id
)*/

select 
    *
from order_info
