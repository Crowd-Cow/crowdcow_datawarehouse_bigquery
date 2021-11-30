with

bids as ( select * from {{ ref('stg_cc__bids') }} )
,bid_items as (select * from {{ ref('stg_cc__bid_items') }} where dbt_valid_to is null)

,order_item_joins as (
    select
        bids.order_id
        ,bids.bid_id
        ,bids.bid_item_id
        ,bids.bid_token
        ,bids.product_id
        ,bids.product_name
        ,bid_items.bid_item_name
        ,bid_items.bid_item_type
        ,bid_items.bid_item_subtype
        ,bids.autofill_reason
        ,bids.fill_type
        ,coalesce(bids.bid_list_price_usd,bids.item_price_usd) as bid_list_price_usd
        ,coalesce(bids.item_price_usd, bids.bid_price_paid_usd) as bid_price_paid_usd
        ,bids.bid_non_member_price_usd
        ,bids.bid_member_price_usd
        ,bids.bid_quantity
        ,bids.created_at_utc
        ,bids.updated_at_utc
        ,bids.first_stuck_at_utc
    from bids
        left join bid_items on bids.bid_item_id = bid_items.bid_item_id
)

,order_item_revenue_calculations as (
    select
        order_id
        ,bid_id
        ,bid_item_id
        ,bid_token
        ,product_id
        ,product_name
        ,bid_item_name
        ,bid_item_type
        ,bid_item_subtype
        ,autofill_reason
        ,fill_type
        ,bid_list_price_usd
        ,bid_price_paid_usd
        ,bid_list_price_usd * bid_quantity as order_item_revenue
        ,(bid_list_price_usd - bid_price_paid_usd) * bid_quantity as order_item_discount
        ,bid_non_member_price_usd
        ,bid_member_price_usd
        ,bid_quantity
        ,created_at_utc
        ,updated_at_utc
        ,first_stuck_at_utc
    from order_item_joins
)

select * from order_item_revenue_calculations
