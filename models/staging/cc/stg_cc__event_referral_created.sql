{{
  config(
    tags=["events"]
  )
}}

with base as (
  
  select * from {{ ref('base_cc__ahoy_events') }}

),

event_referral_created as (

  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments               as experiments
    ,event_json:member::boolean           as is_member
    ,{{ cents_to_usd('event_json:earned_amount') }} as earned_amount_usd 
    ,event_json:referred_by_user_id::int  as referred_by_user_id
    ,event_json:referred_user_id::int     as referred_user_id
    ,event_json:referring_order_id::int   as referring_order_id
  from 
    base
  where 
    event_name = 'referral_created'

)

select * from event_referral_created
