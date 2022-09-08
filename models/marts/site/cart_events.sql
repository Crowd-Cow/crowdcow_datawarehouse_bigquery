with

cart_events as ( select * from {{ ref('events') }} where event_name in ('ORDER_ADD_TO_CART','ORDER_REMOVE_FROM_CART','VIEWED_PRODUCT','PRODUCT_CARD_QUICK_ADD_TO_CART') )
,bid_item as ( select * from {{ ref('bid_items') }} where dbt_valid_to is null )
,product as ( select * from {{ ref('products') }} )

,get_fields as (
    select
        cart_events.event_id
        ,cart_events.visit_id
        ,cart_events.user_id
        ,cart_events.occurred_at_utc
        ,cart_events.event_name
        ,cart_events.price as item_price
        ,ifnull(cart_events.quantity,1) as item_quantity
        ,cart_events.price * item_quantity as item_amount
        ,cart_events.order_id   
        ,iff(event_name = 'PRODUCT_CARD_QUICK_ADD_TO_CART',bid_item.bid_item_id,cart_events.bid_item_id) as bid_item_id
        ,cart_events.product_token
        ,cart_events.title as product_title  
        ,cart_events.name as bid_item_name
        ,cart_events.quantity_sellable
    from cart_events
        left join bid_item on lower(cart_events.event_properties_id) = bid_item.bid_item_token
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
        ,product.product_id
        ,coalesce(get_fields.product_title,product.product_title,get_fields.bid_item_name) as product_title
        ,bid_item_name
        ,quantity_sellable
        ,event_name = 'VIEWED_PRODUCT' and quantity_sellable = 0 as is_oos_view
    from get_fields
        left join product on get_fields.product_token = product.product_token
            and product.dbt_valid_to is null
)

select * from get_product_details
