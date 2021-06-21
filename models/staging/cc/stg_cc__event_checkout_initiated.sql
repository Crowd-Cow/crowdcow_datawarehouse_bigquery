{{
  config(
    tags=["events"]
  )
}}

with base as (
  
  select * from {{ ref('base_cc__ahoy_events') }}

),

event_checkout_initiated as (

  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments     as experiments
    ,event_json:member::boolean as is_member
    ,event_json:value::text     as value
  from 
    base
  where 
    event_name = 'custom_event'
    and event_json:action::text = 'reached-step' 
    and event_json:label::text = '1'

)

select * from event_checkout_initiated
