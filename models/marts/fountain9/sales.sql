with

order_item as ( select * from {{ ref('order_item_details') }})
,sku as ( select * from {{ ref('skus') }} )
,order_detail as ( select * from {{ ref('orders') }} )
,fc as ( select * from {{ ref('fcs') }} )

,order_item_skus as (
    select
        order_item.order_id
        ,sku.category
        ,sku.sub_category
        ,sku.cut_id
        ,sku.cut_name
        ,order_item.bid_sku_quantity
        ,order_item.sku_net_product_revenue
    from order_item
        left join sku on order_item.sku_key = sku.sku_key
)

,order_fc as (
    select
        order_detail.order_id
        ,order_detail.order_paid_at_utc
        ,fc.fc_name
    from order_detail
        left join fc on order_detail.fc_key = fc.fc_key
    where is_paid_order
        and not is_cancelled_order
)
select
    order_fc.order_paid_at_utc::date as order_paid_date
    ,order_fc.fc_name
    ,order_item_skus.category
    ,order_item_skus.sub_category
    ,order_item_skus.cut_name
    ,sum(order_item_skus.bid_sku_quantity) as quantity_sold
    ,sum(order_item_skus.sku_net_product_revenue) as revenue
from order_item_skus
    inner join order_fc on order_item_skus.order_id = order_fc.order_id
group by 1,2,3,4,5