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
event_custom_event as (
  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments     as experiments
    ,event_json:member::boolean as is_member
    ,event_json:action::text    as event_action
    ,event_json:category::text  as event_category
    ,event_json:label::text     as event_label
    ,event_json:value::text     as event_value
  from 
    base
  where 
    event_name = 'custom_event'
)

select * from event_custom_event
