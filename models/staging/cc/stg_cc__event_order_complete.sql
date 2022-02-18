{{
  config(
    materialized = 'incremental',
    unique_key = 'event_id',
    tags=["events"],
    enabled = false
  )
}}
    
with base as (
  
  select * 
  from {{ ref('base_cc__ahoy_events') }} as ae
  where event_name = 'order_complete' 

    {% if is_incremental() %}
      and ae.occurred_at_utc >= coalesce((select max(occurred_at_utc) from {{ this }}), '1900-01-01')
    {% endif %}
    
),

event_order_complete as (

  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments           as experiments
    ,event_json:member::boolean       as is_member
    ,event_json:"$event_id"::text  as order_token
    ,event_json:brands                as bid_item_brands
    ,event_json:categories            as bid_item_categories
    ,{{ clean_strings('event_json:currency::text') }}     as currency
    ,case 
      when event_json:discount::text like '$%' then try_to_decimal(event_json:discount::text, '$9,999.99', 7, 2) -- Some values are dollars like $1.23 and others are cents like 123
      else {{ cents_to_usd('event_json:discount') }}
     end as total_discount_usd
    ,event_json:eligible_for_recurring::boolean     as is_eligible_for_recurring
    ,event_json:estimated_order_arrival_date::date  as estimated_order_arrival_date
    ,event_json:gift_order::boolean   as is_gift_order
    ,try_to_number(event_json:order_id::text)::int  as order_id -- A few order_id are actually order_token and will be null after failing to cast to int
    ,event_json:product_names         as product_names
    ,event_json:products              as products
    ,event_json:recurring::boolean    as is_recurring
    ,case 
      when event_json:shipping::text like '$%' then try_to_decimal(event_json:shipping::text, '$9,999.99', 7, 2) -- Some values are dollars like $1.23 and others are cents like 123
      else {{ cents_to_usd('event_json:shipping') }}
     end as shipping_usd
    ,event_json:suggested_add_ons         as suggested_add_ons
    ,{{ cents_to_usd('event_json:tax') }} as tax_usd
    ,case 
      when event_json:total::text like '$%' then try_to_decimal(event_json:total::text, '$9,999.99', 7, 2) -- Some values are dollars like $1.23 and others are cents like 123
      else {{ cents_to_usd('event_json:total') }}
     end as total_usd
  from
    base

)

select * from event_order_complete
