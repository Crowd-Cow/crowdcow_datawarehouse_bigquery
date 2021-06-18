{{
  config(
    tags=["events"]
  )
}}

with base as (
  select
    *
  from
    {{ ref('base_cc__ahoy_events') }}
),
event_order_add_to_cart as (
  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments           as experiments
    ,event_json:member                as is_member
    ,{{ cents_to_dollars('event_json:amount') }}  as amount_dollars
    ,event_json:bid_item_id::int      as bid_item_id
    ,event_json:brands                as brands
    ,event_json:categories            as categories
    ,event_json:gift_order::boolean   as gift_order
    ,event_json:image_url::text       as image_url
    ,event_json:name::text            as name
    ,event_json:order_id::int         as order_id
    ,{{ cents_to_dollars('event_json:price') }}   as price_dollars
    ,event_json:quantity::int         as quantity
    ,event_json:sku::text             as sku
    ,event_json:url::text             as url
    ,event_json:user_id::int          as user_id
    ,event_json:variant::text         as variant
  from 
    base
  where 
    event_name = 'order_add_to_cart'
)

select * from event_order_add_to_cart
