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
event_order_viewed_by_customer as (
  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments         as experiments
    ,event_json:member::boolean     as is_member
    ,event_json:order_id::int       as order_id
    ,event_json:order_token::text   as order_token
    ,event_json:user_id::int        as user_id
    ,event_json:user_token::text    as user_token
  from 
    base
  where 
    event_name = 'order_viewed_by_customer'
)

select * from event_order_viewed_by_customer
