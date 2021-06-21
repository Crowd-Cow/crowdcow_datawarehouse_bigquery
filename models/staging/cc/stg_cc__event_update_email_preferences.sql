{{
  config(
    tags=["events"]
  )
}}

with base as (
  
  select * from {{ ref('base_cc__ahoy_events') }}

),

event_update_email_preferences as (

  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments                       as experiments
    ,event_json:member::boolean                   as is_member
    ,event_json:email_freq_weekly::boolean        as email_freq_weekly
    ,event_json:email_unsubscribed_all::boolean   as email_unsubscribed_all
  from 
    base
  where 
    event_name = 'update_email_preferences'

)

select * from event_update_email_preferences
