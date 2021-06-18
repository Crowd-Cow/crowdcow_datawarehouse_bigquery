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
event_follow_farm as (
  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments     as experiments
    ,event_json:member::boolean as is_member
    ,event_json:farm_id::int    as farm_id
    ,event_json:meat_type::text as meat_type
  from 
    base
  where 
    event_name = 'follow_farm'
)

select * from event_follow_farm

