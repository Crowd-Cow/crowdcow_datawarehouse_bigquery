{
  config(
    tags=["events"]
  )
}

with base as (
  select
    *
  from
    { ref('base_cc__ahoy_events') }
),
event_viewed_product as (
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
    event_name = 'viewed_product'
)

select * from event_viewed_product

