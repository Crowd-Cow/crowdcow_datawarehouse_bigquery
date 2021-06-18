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
event_redeem_partner_offer as (
  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments         as experiments
    ,event_json:member::boolean     as is_member
    ,event_json:email::text         as email
    ,event_json:partner_path::text  as partner_path
  from 
    base
  where 
    event_name = 'redeem_partner_offer'
)

select * from event_redeem_partner_offer
