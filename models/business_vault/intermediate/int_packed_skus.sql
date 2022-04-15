with

order_packed_sku as ( select * from {{ ref('stg_cc__order_packed_skus') }} )
,sku_reservation as ( select * from {{ ref('stg_cc__sku_reservations') }} where dbt_valid_to is null )
,bid as ( select * from {{ ref('order_items') }} )
,bid_item_sku as ( select distinct bid_item_id,is_single_sku_bid_item,adjusted_dbt_valid_from,adjusted_dbt_valid_to from {{ ref('int_bid_item_skus') }} )
,sku as ( select * from {{ ref('stg_cc__skus') }} )
,sku_box as ( select * from {{ ref('stg_cc__sku_boxes') }} )
,lot as ( select * from {{ ref('stg_cc__lots') }} )
,vendor as ( select * from {{ ref('stg_cc__sku_vendors') }} )
,fc as ( select * from {{ ref('stg_cc__fcs') }} )

,order_packed_items as (
    select
        order_packed_sku.order_id
        ,order_packed_sku.sku_box_id
        ,sku_reservation.bid_id
        ,sku_reservation.bid_item_id
        ,sku_reservation.fc_id
        ,fc.fc_key
        ,order_packed_sku.sku_id
        ,sku_reservation.manually_changed_at_utc is not null as was_manually_changed
        ,sum(order_packed_sku.sku_quantity) as sku_quantity
        ,max(order_packed_sku.created_at_utc) as created_at_utc
        ,max(order_packed_sku.updated_at_utc) as updated_at_utc
    from order_packed_sku
    inner join sku_reservation on order_packed_sku.sku_reservation_id = sku_reservation.sku_reservation_id
    left join fc on sku_reservation.fc_id = fc.fc_id
        and order_packed_sku.created_at_utc >= fc.adjusted_dbt_valid_from
        and order_packed_sku.created_at_utc < fc.adjusted_dbt_valid_to
    group by 1,2,3,4,5,6,7,8
)

,get_bid_details as (
    select
        order_packed_items.*
        ,bid.bid_quantity
        ,bid.promotion_id
        ,bid.bid_item_name
        ,zeroifnull(bid.bid_list_price_usd) as bid_list_price_usd
        ,zeroifnull(bid.bid_gross_product_revenue) as bid_gross_product_revenue
        ,zeroifnull(bid.item_member_discount) as item_member_discount
        ,zeroifnull(bid.item_merch_discount) as item_merch_discount
        ,zeroifnull(bid.item_promotion_discount) as item_promotion_discount
        ,coalesce(bid_item_sku.is_single_sku_bid_item,TRUE) as is_single_sku_bid_item
        ,coalesce(bid.created_at_utc,order_packed_items.created_at_utc) as item_created_at_utc
    from order_packed_items
        left join bid on order_packed_items.bid_id = bid.bid_id
        left join bid_item_sku on order_packed_items.bid_item_id = bid_item_sku.bid_item_id
            and bid.created_at_utc >= bid_item_sku.adjusted_dbt_valid_from
            and bid.created_at_utc < bid_item_sku.adjusted_dbt_valid_to
)

,get_sku_key as (
    select
        get_bid_details.*
        ,sku.sku_key
    from get_bid_details
        left join sku on get_bid_details.sku_id = sku.sku_id
            and get_bid_details.item_created_at_utc >= sku.adjusted_dbt_valid_from
            and get_bid_details.item_created_at_utc < sku.adjusted_dbt_valid_to
)

,get_box_lot_details as (
    select
        get_sku_key.*
        ,sku_box.sku_box_key
        ,coalesce(lot.owner_id,sku_box.owner_id,91) as sku_owner_id
    from get_sku_key
        left join sku_box on get_sku_key.sku_box_id = sku_box.sku_box_id
            and get_sku_key.created_at_utc >= sku_box.adjusted_dbt_valid_from
            and get_sku_key.created_at_utc < sku_box.adjusted_dbt_valid_to
        left join lot on sku_box.lot_id = lot.lot_id
)

select 
    {{ dbt_utils.surrogate_key(['order_id','bid_id','bid_item_id','sku_id','sku_box_id']) }} as order_item_detail_id
    ,order_id
    ,bid_id
    ,bid_item_id
    ,sku_id
    ,sku_key
    ,sku_box_id
    ,sku_box_key
    ,sku_owner_id
    ,fc_id
    ,fc_key
    ,promotion_id
    ,bid_item_name
    ,bid_quantity
    ,sku_quantity
    ,bid_list_price_usd
    ,bid_gross_product_revenue
    ,item_member_discount
    ,item_merch_discount
    ,item_promotion_discount
    ,is_single_sku_bid_item
    ,true as is_item_packed
    ,was_manually_changed
    ,created_at_utc
    ,updated_at_utc
from get_box_lot_details
