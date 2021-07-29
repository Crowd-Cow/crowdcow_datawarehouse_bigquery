{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    tags=["events"]
  )
}}
    
with base as (
  
  select * 
  from {{ ref('base_cc__ahoy_events') }} as ae
  where true 

    {% if is_incremental() %}
      and ae.occurred_at_utc >= coalesce((select max(occurred_at_utc) from {{ this }}), '1900-01-01')
    {% endif %}
    
),

event_order_paid as (

  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments           as experiments
    ,event_json:member::boolean       as is_member
    ,{{ clean_strings('event_json:"$event_id"') }}  as order_token
    ,event_json:brands                as bid_item_brands
    ,event_json:categories            as bid_item_categories
    ,{{ clean_strings('event_json:currency::text') }}   as currency
    ,{{ cents_to_usd('event_json:discount') }}          as total_discount_usd
    ,event_json:eligible_for_recurring::boolean         as is_eligible_for_recurring
    ,event_json:estimated_order_arrival_date::date      as estimated_order_arrival_date
    ,event_json:gift_order::boolean   as is_gift_order
    ,event_json:order_id::int         as order_id
    ,event_json:product_names         as product_names
    ,event_json:products              as products
    ,event_json:recurring::boolean    as is_recurring
    ,{{ cents_to_usd('event_json:shipping') }}  as shipping_usd
    ,event_json:suggested_add_ons               as suggested_add_ons
    ,{{ cents_to_usd('event_json:tax') }}       as tax_usd
    ,{{ cents_to_usd('event_json:total') }}     as total_usd
  from 
    base
  where 
    event_name = 'order_paid'

)

select * from event_order_paid
