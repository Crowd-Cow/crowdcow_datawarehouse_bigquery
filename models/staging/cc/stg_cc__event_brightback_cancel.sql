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
event_brightback_cancel as (
  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments       as experiments
    ,event_json:member::boolean   as is_member
    ,event_json:app_id::text      as app_id
    ,event_json:context           as context 
    ,event_json:fields            as fields 
    ,event_json:id::text          as id 
    ,event_json:name::text        as name 
    ,event_json:session_id::text  as session_id 
    ,event_json:survey            as survey
    ,event_json:timestamp::timestamp_ntz  as brightback_timestamp 
    ,event_json:type::text        as type
  from 
    base
  where 
    event_name = 'brightback_cancel'
)

select * from event_brightback_cancel
