with

order_item as ( select * from {{ ref('order_items') }} )
,bid_item_sku as ( select * from {{ ref('int_bid_item_skus') }} )

,join_bid_item_skus as (
    select  
        order_item.order_id
        ,order_item.bid_id
        ,order_item.bid_item_id
        ,order_item.promotion_id
        ,order_item.bid_item_name
        ,order_item.bid_quantity
        ,order_item.bid_list_price_usd
        ,order_item.bid_gross_product_revenue
        ,order_item.item_member_discount * -1 as item_member_discount
        ,order_item.item_merch_discount * -1 as item_merch_discount
        ,order_item.item_promotion_discount * -1 as item_promotion_discount
        ,order_item.created_at_utc as bid_created_at_utc
        ,order_item.updated_at_utc as bid_updated_at_utc
        ,bid_item_sku.sku_id
        ,bid_item_sku.sku_quantity
        ,bid_item_sku.is_single_sku_bid_item
        ,order_item.bid_quantity * bid_item_sku.sku_quantity as bid_sku_quantity
    from order_item
        left join bid_item_sku on order_item.bid_item_id = bid_item_sku.bid_item_id
            and order_item.created_at_utc >= bid_item_sku.adjusted_dbt_valid_from
            and order_item.created_at_utc < bid_item_sku.adjusted_dbt_valid_to
)

select * from join_bid_item_skus
