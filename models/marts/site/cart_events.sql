with

cart_events as ( select * from {{ ref('events') }} where event_name in ('ORDER_ADD_TO_CART','ORDER_REMOVE_FROM_CART') )
,bid_item as ( select * from {{ ref('bid_items') }} )

,get_fields as (
    select
        event_id
        ,visit_id
        ,user_id
        ,occurred_at_utc
        ,event_name
        ,price as item_price
        ,quantity as item_quantity
        ,price * quantity as item_amount
        ,order_id
        ,bid_item_id
    from cart_events
)

,get_bid_item_key as (
    select
        get_fields.event_id
        ,get_fields.visit_id
        ,get_fields.user_id
        ,get_fields.occurred_at_utc
        ,get_fields.event_name
        ,get_fields.item_price
        ,get_fields.item_quantity
        ,get_fields.item_amount
        ,get_fields.order_id
        ,get_fields.bid_item_id
        ,bid_item.bid_item_key
    from get_fields
        left join bid_item on get_fields.bid_item_id = bid_item.bid_item_id
            and get_fields.occurred_at_utc >= adjusted_dbt_valid_from
            and get_fields.occurred_at_utc < adjusted_dbt_valid_to
)

select * from get_bid_item_key
