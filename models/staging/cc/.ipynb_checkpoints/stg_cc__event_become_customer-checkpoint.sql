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
event_become_customer as (
  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    
  from 
    base
  where 
    event_name = 'become_customer'
)

select * from event_become_customer

