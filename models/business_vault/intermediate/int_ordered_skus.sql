with

order_item as ( select * from {{ ref('order_items') }} )
,bid_item_sku as ( select distinct bid_item_id,sku_id,sku_quantity,is_single_sku_bid_item,adjusted_dbt_valid_from,adjusted_dbt_valid_to from {{ ref('int_bid_item_skus') }} )
,order_fc as ( select order_id,fc_id from {{ ref('stg_cc__orders') }} )
,fc as ( select * from {{ ref('stg_cc__fcs') }} )
,sku as ( select * from {{ ref('stg_cc__skus') }} )

,breakout_skus as (
    select  
        order_item.order_id
        ,order_item.bid_id
        ,order_item.bid_item_id
        ,order_item.promotion_id
        ,order_item.bid_item_name
        ,order_item.bid_quantity
        ,order_item.bid_list_price_usd
        ,order_item.bid_gross_product_revenue
        ,order_item.item_member_discount
        ,order_item.item_merch_discount
        ,order_item.item_promotion_discount
        ,order_item.created_at_utc
        ,order_item.updated_at_utc
        ,bid_item_sku.sku_id
        ,bid_item_sku.sku_quantity as item_sku_quantity
        ,bid_item_sku.is_single_sku_bid_item
        ,order_item.bid_quantity * bid_item_sku.sku_quantity as sku_quantity
    from order_item
        left join bid_item_sku on order_item.bid_item_id = bid_item_sku.bid_item_id
            and order_item.created_at_utc >= bid_item_sku.adjusted_dbt_valid_from
            and order_item.created_at_utc < bid_item_sku.adjusted_dbt_valid_to
)

,get_fc_key as (
    select
        breakout_skus.*
        ,order_fc.fc_id
        ,fc.fc_key
    from breakout_skus
        inner join order_fc on breakout_skus.order_id = order_fc.order_id
        left join fc on order_fc.fc_id = fc.fc_id
            and breakout_skus.created_at_utc >= fc.adjusted_dbt_valid_from
            and breakout_skus.created_at_utc < fc.adjusted_dbt_valid_to
)

,get_sku_key as (
    select
        get_fc_key.*
        ,sku.sku_key
    from get_fc_key
        left join sku on get_fc_key.sku_id = sku.sku_id
            and get_fc_key.created_at_utc >= sku.adjusted_dbt_valid_from
            and get_fc_key.created_at_utc < sku.adjusted_dbt_valid_to
            
)

select 
    {{ dbt_utils.surrogate_key(['order_id','bid_id','bid_item_id','sku_id']) }} as order_item_detail_id
    ,order_id
    ,bid_id
    ,bid_item_id
    ,sku_id
    ,sku_key

    /*** The SKU box and owner information for an order item is not known until the order is packed. ***/
    ,null::int as sku_box_id
    ,null::text as sku_box_key
    ,null::int as sku_owner_id
    
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
    ,false as is_item_packed
    ,created_at_utc
    ,updated_at_utc
from get_sku_key
