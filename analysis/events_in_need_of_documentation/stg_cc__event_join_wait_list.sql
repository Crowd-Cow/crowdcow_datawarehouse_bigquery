{{
  config(
    tags=["events"],
    enabled=false
  )
}}

with base as (
  
  select * from {{ ref('base_cc__ahoy_events') }}

),

event_join_wait_list as (

  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments           as experiments
    ,event_json:member::boolean       as is_member
    ,event_json:bid_item_token::text  as bid_item_token
    ,event_json:product               as product_json
  from 
    base
  where 
    event_name = 'join_wait_list'

)

select * from event_join_wait_list
