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
event_loyalty_reward_added_to_order as (
  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments         as experiments
    ,event_json:member::boolean     as is_member
    ,event_json:bid_item_name::text as bid_item_name
    ,event_json:order               as order
    ,event_json:order_estimated_arrival_date::date  as order_estimated_arrival_date
    ,event_json:order_fulfillment_date::date        as order_fulfillment_date
    ,event_json:renewal_order_count::int            as renewal_order_count
    ,event_json:renewal_period::int       as renewal_period
    ,event_json:subscription_id::text     as subscription_id
    ,event_json:subscription_token::text  as subscription_token
  from 
    base
  where 
    event_name = 'loyalty_reward_added_to_order'
)

select * from event_loyalty_reward_added_to_order

