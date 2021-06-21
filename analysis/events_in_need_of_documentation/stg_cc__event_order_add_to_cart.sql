{{
  config(
    tags=["events"],
    enabled=false
  )
}}

with base as (
  
  select * from {{ ref('base_cc__ahoy_events') }}

),

event_order_add_to_cart as (

  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments           as experiments
    ,event_json:member::boolean       as is_member
    ,{{ cents_to_usd('event_json:amount') }}  as amount_usd
    ,event_json:bid_item_id::int      as bid_item_id
    ,event_json:brands                as brands
    ,event_json:categories            as categories
    ,event_json:gift_order::boolean   as gift_order
    ,event_json:image_url::text       as image_url
    ,event_json:name::text            as product_name
    ,event_json:order_id::int         as order_id
    ,{{ cents_to_usd('event_json:price') }}   as price_usd
    ,event_json:quantity::int         as order_quantity
    ,event_json:sku::text             as sku_name
    ,event_json:url::text             as url
    ,event_json:variant::text         as variant
  from 
    base
  where 
    event_name = 'order_add_to_cart'

)

select * from event_order_add_to_cart
