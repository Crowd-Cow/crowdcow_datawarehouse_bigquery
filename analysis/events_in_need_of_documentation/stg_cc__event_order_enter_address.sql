{{
  config(
    tags=["events"],
    enabled=false
  )
}}

with base as (
  
  select * from {{ ref('base_cc__ahoy_events') }}

),

event_order_enter_address as (

  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments     as experiments
    ,event_json:member::boolean as is_member
    ,event_json:order_id::int   as order_id
  from 
    base
  where 
    event_name = 'order_enter_address'

)

select * from event_order_enter_address
