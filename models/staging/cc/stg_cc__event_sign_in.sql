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
event_sign_in as (
  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments                 as experiments
    ,event_json:member::boolean             as is_member
  from 
    base
  where 
    event_name = 'sign_in'
)

select * from event_sign_in

