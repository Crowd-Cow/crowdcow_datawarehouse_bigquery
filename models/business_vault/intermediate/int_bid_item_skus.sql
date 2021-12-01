with

bid_item as ( select * from {{ ref('stg_cc__bid_items') }} )
,bid_item_sku_quantity as ( select * from  {{ ref('stg_cc__bid_item_sku_with_quantities') }} )

,bid_item_sku_count as (
    select
        bid_item_id
        ,sku_id
        ,sku_quantity
        ,adjusted_dbt_valid_from
        ,adjusted_dbt_valid_to
        ,count(distinct sku_id) over(partition by bid_item_id) as bid_item_sku_count
    from bid_item_sku_quantity
)

,bid_item_joins as (
    select
        bid_item.bid_item_id
        ,bid_item.bid_item_key
        ,bid_item.created_at_utc as bid_item_created_at_utc
        ,bid_item.updated_at_utc as bid_item_updated_at_utc
        ,bid_item.adjusted_dbt_valid_from
        ,bid_item.adjusted_dbt_valid_to
        ,bid_item.bid_item_name
        ,bid_item_price_usd
        ,bid_item_sku_count.sku_id
        ,bid_item_sku_count.sku_quantity
        ,bid_item_sku_count.bid_item_sku_count = 1 as is_single_sku_bid_item
    from bid_item
        left join bid_item_sku_count on bid_item.bid_item_id = bid_item_sku_count.bid_item_id
            and bid_item.updated_at_utc >= bid_item_sku_count.adjusted_dbt_valid_from
            and bid_item.updated_at_utc < bid_item_sku_count.adjusted_dbt_valid_to
)

select * from bid_item_joins
