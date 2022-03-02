with

order_packed_sku as ( select * from {{ ref('stg_cc__order_packed_skus') }} )
,sku_reservation as ( select * from {{ ref('stg_cc__sku_reservations') }} )
,sku as ( select * from {{ ref('skus') }} )

,order_packed_items as (
    select
        order_packed_sku.order_id
        ,sku_reservation.bid_id
        ,sku_reservation.bid_item_id
        ,order_packed_sku.sku_id
        ,sku.sku_key as packed_sku_key
        ,sku_reservation.bid_item_id is null as is_packed_item_only
        ,sku_reservation.original_quantity as packed_sku_quantity
        ,sku_reservation.sku_reservation_quantity
        ,sku.sku_price_usd * sku_reservation.original_quantity as packed_sku_price
        ,sku.sku_cost_usd * sku_reservation.original_quantity as packed_sku_cost
        ,order_packed_sku.created_at_utc as packed_created_at_utc
        ,order_packed_sku.updated_at_utc as packed_updated_at_utc
        ,order_packed_sku.sku_quantity as sku_quantity
    from order_packed_sku
    inner join sku_reservation on order_packed_sku.sku_reservation_id = sku_reservation.sku_reservation_id
        and order_packed_sku.created_at_utc >= sku_reservation.adjusted_dbt_valid_from
        and order_packed_sku.created_at_utc < sku_reservation.adjusted_dbt_valid_to
    left join sku on order_packed_sku.sku_id = sku.sku_id
        and order_packed_sku.created_at_utc >= sku.adjusted_dbt_valid_from
        and order_packed_sku.created_at_utc < sku.adjusted_dbt_valid_to
)

,remove_dups as (
    select
        order_id
        ,bid_id
        ,bid_item_id
        ,sku_id
        ,packed_sku_key
        ,is_packed_item_only
        ,packed_sku_quantity
        ,packed_sku_price
        ,packed_sku_cost
        ,packed_created_at_utc
        ,packed_updated_at_utc
    from order_packed_items
    qualify row_number() over(partition by order_id,bid_id,bid_item_id,sku_id order by packed_created_at_utc desc) = 1
)

select * from remove_dups
