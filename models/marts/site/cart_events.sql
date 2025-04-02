{% set partitions_to_replace = [
  'timestamp(current_date)',
  'timestamp(date_sub(current_date, interval 1 day))'
] %}
{{
  config(
        materialized = 'incremental',
        partition_by = {'field': 'occurred_at_utc', 'data_type': 'timestamp'},
        cluster_by = ['visit_id','user_id','event_name'],
        incremental_strategy = 'insert_overwrite',
        partitions = partitions_to_replace,
        on_schema_change = 'sync_all_columns'
    )
}}
with

cart_events as ( 
    select 
     event_id
        ,visit_id
        ,user_id
        ,occurred_at_utc
        ,event_name
        ,on_page_path
        ,page_section
        ,price 
        ,quantity
        ,order_id   
        ,bid_item_id
        ,product_token
        ,title 
        ,name 
        ,quantity_sellable
        ,event_properties_id
        ,pdc_in_stock
        ,pdp_in_stock
        ,brands
        ,categories
    from {{ ref('events') }}
    where event_name in ('ORDER_ADD_TO_CART','ORDER_REMOVE_FROM_CART','VIEWED_PRODUCT','PRODUCT_CARD_VIEWED','PRODUCT_CARD_QUICK_ADD_TO_CART')
{% if is_incremental() %}
     and timestamp_trunc(occurred_at_utc, day) in ({{ partitions_to_replace | join(',') }})
  {% endif %}
  )
,bid_item as ( select * from {{ ref('bid_items') }} where dbt_valid_to is null )
,product as ( select * from {{ ref('products') }} )

,get_fields as (
    select
        cart_events.event_id
        ,cart_events.visit_id
        ,cart_events.user_id
        ,cart_events.occurred_at_utc
        ,cart_events.event_name
        ,cart_events.on_page_path
        ,cart_events.page_section
        ,cart_events.price as item_price
        ,ifnull(cart_events.quantity,1) as item_quantity
        ,cart_events.price * ifnull(cart_events.quantity,1) as item_amount
        ,cart_events.order_id   
        ,if(event_name = 'PRODUCT_CARD_QUICK_ADD_TO_CART' or event_name = 'VIEWED_PRODUCT' or event_name = 'PRODUCT_CARD_VIEWED',bid_item.bid_item_id,cart_events.bid_item_id) as bid_item_id
        ,cart_events.product_token
        ,cart_events.title as product_title  
        ,cart_events.name as bid_item_name
        ,cart_events.quantity_sellable
        ,cart_events.pdc_in_stock
        ,cart_events.pdp_in_stock
        ,cart_events.brands
        ,cart_events.categories
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
        ,get_fields.on_page_path
        ,get_fields.page_section
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
        ,pdc_in_stock
        ,pdp_in_stock
        ,brands
        ,categories
    from get_fields
        left join product on get_fields.product_token = product.product_token
            and product.dbt_valid_to is null
)

select * from get_product_details
