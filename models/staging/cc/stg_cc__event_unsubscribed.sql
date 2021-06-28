{{
  config(
    tags=["events"],
    enabled=false
  )
}}

with base as (
  
  select * from {{ ref('base_cc__ahoy_events') }}

),

event_unsubscribed as (

  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments           as experiments
    ,event_json:member::boolean       as is_member
    ,event_json:reason::text          as unsubscribe_reason
    ,event_json:subscription_id::int  as subscription_id
    ,event_json:user_id::int          as user_id_from_event_json  -- base_cc__ahoy_events already has a user_id column
  from 
    base
  where 
    event_name = 'unsubscribed'

)

select * from event_unsubscribed
