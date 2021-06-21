{{
  config(
    tags=["events"]
  )
}}

with base as (
  
  select * from {{ ref('base_cc__ahoy_events') }}

),

event_pdp_added_to_cart as (

  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
  from 
    base
  where 
    event_name = 'custom_event'
      and event_json:category::text = 'product'
      and event_json:action::text = 'page-interaction' 
      and event_json:label::text = 'clicked-add-to-cart'

)

select * from event_pdp_added_to_cart
