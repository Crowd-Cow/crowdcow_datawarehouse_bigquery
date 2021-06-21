{{
  config(
    tags=["events"]
  )
}}

with base as (
  
  select * from {{ ref('base_cc__ahoy_events') }}

),

event_order_cancelled as (

  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments           as experiments
    ,event_json:member::boolean       as is_member
    ,event_json:"$event_id"::text     as event_id_from_json  -- What is this?
    ,{{ cents_to_usd('event_json:"$value"') }}  as value_usd
    ,event_json:brands                as brands
    ,event_json:categories            as categories
    ,event_json:currency::text        as currency
    ,{{ cents_to_usd('event_json:discount') }}  as discount_usd
    ,event_json:eligible_for_recurring::boolean     as eligible_for_recurring
    ,event_json:estimated_order_arrival_date::date  as estimated_order_arrival_date
    ,event_json:gift_order::boolean   as gift_order
    ,event_json:order_id::int         as order_id
    ,event_json:product_names         as product_names
    ,event_json:products              as products
    ,event_json:recurring::boolean    as recurring
    ,{{ cents_to_usd('event_json:shipping') }}  as shipping_usd
    ,event_json:suggested_add_ons     as suggested_add_ons
    ,{{ cents_to_usd('event_json:tax') }}       as tax_usd
    ,{{ cents_to_usd('event_json:total') }}     as total_usd
  from 
    base
  where 
    event_name = 'order_cancelled'

)

select * from event_order_cancelled
