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
        ,sku.sku_id
        ,sku.sku_name
        ,order_item.bid_sku_quantity
        ,order_item.sku_gross_product_revenue
        ,order_item.sku_net_product_revenue
        ,order_item.bid_list_price_usd
        ,order_item.bid_sku_price
        ,order_item.sku_price_proportion
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
    where order_detail.is_paid_order
        and not order_detail.is_cancelled_order
        and not order_detail.is_bulk_gift_order
        and order_detail.order_paid_at_utc >= '2019-01-01'
        and order_detail.fc_id not in (8,10,14) --filter out Poseidon, Nationwide, and Valmeyer FCs
)
select
    order_fc.order_paid_at_utc::date as order_paid_date
    ,order_fc.fc_name
    ,order_item_skus.category
    ,order_item_skus.sub_category
    ,order_item_skus.cut_id
    ,order_item_skus.cut_name
    ,sum(order_item_skus.bid_sku_quantity) as quantity_sold
    ,sum(order_item_skus.sku_gross_product_revenue) as revenue
    ,round(avg(div0(order_item_skus.bid_sku_price,bid_sku_quantity)),2) as avgerage_list_price
from order_item_skus
    inner join order_fc on order_item_skus.order_id = order_fc.order_id
where cut_id is not null
    and category is not null
group by 1,2,3,4,5,6
