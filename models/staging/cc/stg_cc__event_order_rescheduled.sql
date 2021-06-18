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
event_order_rescheduled as (
  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments           as experiments
    ,event_json:member::boolean       as is_member
    ,event_json:"$event_id"::text     as event_id_from_json  -- What is this?
    ,event_json:gift_order::boolean   as gift_order
    ,event_json:new_scheduled_fulfillment_date::date  as new_scheduled_fulfillment_date
    ,event_json:old_scheduled_fulfillment_date::date  as old_scheduled_fulfillment_date
    ,event_json:order_id::int         as order_id
    ,event_json:order_token::text     as order_token
    ,event_json:reason::text          as order_reschedule_reason
    ,event_json:user_id::int          as user_id_from_event_json  -- base_cc__ahoy_events already has a user_id column
    ,event_json:user_making_change_id::int            as user_making_change_id
    ,event_json:user_token::text      as user_token
  from 
    base
  where 
    event_name = 'order_rescheduled'
)

select * from event_order_rescheduled
