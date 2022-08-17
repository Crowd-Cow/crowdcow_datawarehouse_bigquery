with

cart_events as ( select * from {{ ref('events') }} where event_name in ('ORDER_ADD_TO_CART','ORDER_REMOVE_FROM_CART','VIEWED_PRODUCT','PRODUCT_CARD_QUICK_ADD_TO_CART') )
,bid_item as ( select * from {{ ref('bid_items') }} )
,product as ( select * from {{ ref('products') }} )

,get_fields as (
    select
        event_id
        ,visit_id
        ,user_id
        ,occurred_at_utc
        ,event_name
        ,price as item_price
        ,ifnull(quantity,1) as item_quantity
        ,price * item_quantity as item_amount
        ,order_id
        ,bid_item_id
        ,product_token
        ,title as product_title  
        ,name as bid_item_name
    from cart_events
)

,get_product_details as (
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
        ,get_fields.product_token
        ,coalesce(get_fields.product_title,product.product_title,get_fields.bid_item_name) as product_title
        ,bid_item_name
    from get_fields
        left join product on get_fields.product_token = product.product_token
            and product.dbt_valid_to is null
)

select * from get_product_details
