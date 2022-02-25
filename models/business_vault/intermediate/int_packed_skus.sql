with

order_packed_sku as ( select * from {{ ref('stg_cc__order_packed_skus') }} )
,sku_reservation as ( select * from {{ ref('stg_cc__sku_reservations') }} )
,order_item as ( select * from {{ ref('order_items') }} )

,order_packed_reservations as (
    select
        order_packed_sku.order_id
        ,order_packed_sku.sku_id
        ,sku_reservation.bid_id
        ,sku_reservation.bid_item_id
        ,max(order_packed_sku.created_at_utc) as created_at_utc
        ,max(order_packed_sku.updated_at_utc) as updated_at_utc
        ,sum(order_packed_sku.sku_quantity) as sku_quantity
    from order_packed_sku
    inner join sku_reservation on order_packed_sku.sku_reservation_id = sku_reservation.sku_reservation_id
        and order_packed_sku.created_at_utc >= sku_reservation.adjusted_dbt_valid_from
        and order_packed_sku.created_at_utc < sku_reservation.adjusted_dbt_valid_to
    group by 1,2,3,4
)

,join_order_item as (
    select
        order_packed_reservations.order_id
        ,order_packed_reservations.bid_id
        ,order_packed_reservations.bid_item_id
        ,order_item.promotion_id
        ,order_packed_reservations.sku_id
        ,order_item.bid_item_name
        ,order_item.bid_quantity
        ,order_item.bid_gross_product_revenue
        ,order_item.item_member_discount * -1 as item_member_discount
        ,order_item.item_merch_discount * -1  as item_merch_discount
        ,order_item.item_promotion_discount * -1 as item_promotion_discount
        ,order_packed_reservations.sku_quantity as packed_sku_quantity
        ,count(distinct order_packed_reservations.sku_id) 
            over(partition by order_packed_reservations.order_id,order_packed_reservations.bid_id,order_packed_reservations.bid_item_id) = 1 as is_single_sku_bid_item
        ,order_packed_reservations.bid_id is null as is_packing_item_only
        ,order_packed_reservations.created_at_utc
        ,order_packed_reservations.updated_at_utc
        ,order_item.created_at_utc as bid_created_at_utc
    from order_packed_reservations
    left join order_item on order_packed_reservations.order_id = order_item.order_id
        and order_packed_reservations.bid_id = order_item.bid_id
        and order_packed_reservations.bid_item_id = order_item.bid_item_id
)

select * from join_order_item
