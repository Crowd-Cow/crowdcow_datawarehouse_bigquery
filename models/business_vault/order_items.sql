with

bids as ( select * from {{ ref('stg_cc__bids') }} )
,bid_items as (select * from {{ ref('stg_cc__bid_items') }})

,order_items as (
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
        ,bids.item_price_usd
        ,bids.bid_list_price_usd
        ,bids.bid_non_member_price_usd
        ,bids.bid_price_paid_usd
        ,bids.bid_member_price_usd
        ,bids.bid_quantity
        ,bids.bid_quantity * item_price_usd as order_item_revenue
        ,bids.created_at_utc
        ,bids.updated_at_utc
        ,bids.first_stuck_at_utc
    from bids
        left join bid_items on bids.bid_item_id = bid_items.bid_item_id
)

select * from order_items
