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
event_signup_to_be_notified as (
  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments         as experiments
    ,event_json:member::boolean     as is_member
    ,event_json:capture_to_wait_list_from_path::text  as capture_to_wait_list_from_path
    ,event_json:dev_id::text        as dev_id
    ,event_json:email::text         as email
    ,event_json:landing_page::text  as landing_page
    ,event_json:postal_code::int    as postal_code
    ,event_json:referrer_url::text  as referrer_url
  from 
    base
  where 
    event_name = 'signup_to_be_notified'
)

select * from event_signup_to_be_notified
