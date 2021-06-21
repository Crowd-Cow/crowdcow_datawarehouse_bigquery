{{
  config(
    tags=["events"]
  )
}}

with base as (
  
  select * from {{ ref('base_cc__ahoy_events') }}

),

event_cart_for_wrong_zip_code as (

  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments     as experiments
    ,event_json:member::boolean as is_member
    ,event_json:order::text     as order_token  -- order is a keyword, so can't be used as a column name
  from 
    base
  where 
    event_name = 'cart_for_wrong_zip_code'

)

select * from event_cart_for_wrong_zip_code
