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
event_unfollow_farm as (
  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments as experiments
    ,event_json:member      as is_member
  from 
    base
  where 
    event_name = 'unfollow_farm'
)

select * from event_unfollow_farm

