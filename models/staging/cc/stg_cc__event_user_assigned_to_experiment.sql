{{
  config(
    tags=["events"]
  )
}}

with base as (
  
  select * from {{ ref('base_cc__ahoy_events') }}

),

event_user_assigned_to_experiment as (

  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments               as experiments
    ,event_json:member::boolean           as is_member
    ,event_json:experiment_token::text    as experiment_token
    ,event_json:variant::text             as variant
  from 
    base
  where 
    event_name = 'user_assigned_to_experiment'

)

select * from event_user_assigned_to_experiment
