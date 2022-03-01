with

order_packed_sku as ( select * from {{ ref('stg_cc__order_packed_skus') }} )
,sku_reservation as ( select * from {{ ref('stg_cc__sku_reservations') }} )
,sku as ( select * from {{ ref('skus') }} )

,order_packed_items as (
    select
        order_packed_sku.order_id
        ,sku_reservation.bid_id

        /*** Items that are packed only (e.g. handwritten notes, inserts, etc.) have a null bid item id ***/
        /*** For these items, a `9999` is appended to the front of the `sku_id` to give the item a pseudo id ***/
        /*** The pseudo id allows the aggregation to occur at the bid item level while not losing visibility into the packing only items ***/
        /*** This will also make sure the pseudo bid item id doesn't join to a real bid item id ***/
        ,iff(sku_reservation.bid_item_id is null,9999 || order_packed_sku.sku_id,sku_reservation.bid_item_id) as bid_item_id
        ,sku_reservation.bid_item_id is null as is_packed_item_only

        ,sum(skus.sku_cost_usd) as packed_sku_cost
        ,max(order_packed_sku.created_at_utc) as packed_created_at_utc
        ,max(order_packed_sku.updated_at_utc) as packed_updated_at_utc
        ,sum(order_packed_sku.sku_quantity) as packed_sku_quantity
    from order_packed_sku
    inner join sku_reservation on order_packed_sku.sku_reservation_id = sku_reservation.sku_reservation_id
        and order_packed_sku.created_at_utc >= sku_reservation.adjusted_dbt_valid_from
        and order_packed_sku.created_at_utc < sku_reservation.adjusted_dbt_valid_to
    left join skus on order_packed_sku.sku_id = skus.sku_id
        and order_packed_sku.created_at_utc >= skus.adjusted_dbt_valid_from
        and order_packed_sku.created_at_utc < skus.adjusted_dbt_valid_to
    group by 1,2,3,4
)

select * from order_packed_items
