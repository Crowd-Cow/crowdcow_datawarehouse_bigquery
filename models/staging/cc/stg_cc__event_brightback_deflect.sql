{{
  config(
    tags=["events"]
  )
}}

with base as (
  
  select * from {{ ref('base_cc__ahoy_events') }}

),

event_brightback_deflect as (

  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments       as experiments
    ,event_json:member::boolean   as is_member
    ,event_json:app_id::text      as brightback_app_id
    ,event_json:context           as brightback_context 
    ,event_json:fields            as brightback_fields 
    ,event_json:id::text          as brightback_id 
    ,event_json:name::text        as brightback_name 
    ,event_json:session_id::text  as brightback_session_id 
    ,event_json:survey            as brightback_survey
    ,event_json:timestamp::timestamp_ntz  as brightback_timestamp_utc
    ,event_json:type::text        as brightback_type
  from 
    base
  where 
    event_name = 'brightback_deflect'

)

select * from event_brightback_deflect
