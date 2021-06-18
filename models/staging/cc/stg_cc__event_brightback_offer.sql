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
event_brightback_offer as (
  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments       as experiments
    ,event_json:member::boolean   as is_member
    ,event_json:action::text      as brightback_action
    ,event_json:app_id::text      as brightback_app_id
    ,event_json:context           as brightback_context
    ,event_json:fields            as brightback_fields 
    ,event_json:form              as brightback_form
    ,event_json:id::text          as brightback_id 
    ,event_json:name::text        as brightback_name
    ,event_json:offer::text       as brightback_offer
    ,event_json:session_id::text  as brightback_session_id 
    ,event_json:survey            as brightback_survey
    ,event_json:timestamp::timestamp_ntz  as brightback_timestamp 
    ,event_json:type::text        as brightback_type
    ,event_json:url::text         as brightback_url
  from 
    base
  where 
    event_name = 'brightback_offer'
)

select * from event_brightback_offer
