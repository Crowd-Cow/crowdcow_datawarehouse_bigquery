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
    ,event_json:experiments as experiments
    ,event_json:member      as is_member
    ,{{ cents_to_dollars('event_json:amount') }} as amount_dollars
  from 
    base
  where 
    event_name = 'order_add_to_cart'
)

select * from event_order_add_to_cart
