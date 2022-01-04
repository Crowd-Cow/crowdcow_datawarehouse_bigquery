with

order_item as ( select * from {{ ref('order_items') }} )
,bid_item_sku as ( select * from {{ ref('int_bid_item_skus') }} )
,sku as ( select * from {{ ref('skus') }} )

,join_bid_item_skus as (
    select  
        order_item.order_id
        ,order_item.bid_id
        ,order_item.bid_item_id
        ,order_item.bid_item_name
        ,order_item.bid_quantity
        ,order_item.bid_list_price_usd
        ,order_item.order_item_revenue
        ,order_item.created_at_utc as bid_created_at_utc
        ,order_item.updated_at_utc as bid_updated_at_utc
        ,bid_item_sku.sku_id
        ,bid_item_sku.sku_quantity
        ,bid_item_sku.is_single_sku_bid_item
        ,order_item.bid_quantity * bid_item_sku.sku_quantity as item_sku_quantity
    from order_item
        left join bid_item_sku on order_item.bid_item_id = bid_item_sku.bid_item_id
            and order_item.created_at_utc >= bid_item_sku.adjusted_dbt_valid_from
            and order_item.created_at_utc < bid_item_sku.adjusted_dbt_valid_to
)

,join_historical_sku_info as (
    select
        join_bid_item_skus.*
        ,sku.sku_price_usd
        ,sku.sku_cost_usd
        
        ,case
            when div0(sku.sku_price_usd * join_bid_item_skus.item_sku_quantity,join_bid_item_skus.order_item_revenue)  > 1 
                or join_bid_item_skus.sku_id is null then 1
            else div0(sku.sku_price_usd * join_bid_item_skus.item_sku_quantity,join_bid_item_skus.order_item_revenue)
         end as sku_price_proportion

    from join_bid_item_skus
        left join sku on join_bid_item_skus.sku_id = sku.sku_id
            and join_bid_item_skus.bid_created_at_utc >= sku.adjusted_dbt_valid_from
            and join_bid_item_skus.bid_created_at_utc < sku.adjusted_dbt_valid_to
)

,sku_calculations as (
    select
        *
        ,sku_price_usd * item_sku_quantity as total_sku_price
        
        ,case
            when is_single_sku_bid_item or sku_id is null then order_item_revenue
            else sku_price_proportion * order_item_revenue
         end as sku_product_revenue

    from join_historical_sku_info
)

,add_keys as (
    select
        {{ dbt_utils.surrogate_key( ['order_id','bid_id','bid_item_id','sku_id'] ) }} as order_item_details_id
        ,order_id
        ,bid_id
        ,bid_item_id
        ,sku_id
        ,{{ get_join_key('skus','sku_key','sku_id','sku_calculations','sku_id','bid_created_at_utc') }} as sku_key
        ,bid_item_name
        ,bid_quantity
        ,sku_quantity
        ,item_sku_quantity
        ,bid_list_price_usd
        ,order_item_revenue
        ,sku_price_usd
        ,sku_price_proportion
        ,total_sku_price
        ,sku_product_revenue
        ,is_single_sku_bid_item
        ,bid_created_at_utc
        ,bid_updated_at_utc
    from sku_calculations
)

select * from add_keys
