{{
  config(
    tags=["events"]
  )
}}

with base as (
  
  select * from {{ ref('base_cc__ahoy_events') }}

),

event_order_complete as (

  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments           as experiments
    ,event_json:member::boolean       as is_member
    ,{{ clean_strings('event_json:"$event_id"::text') }}  as order_token
    ,{{ cents_to_usd('event_json:"$value"') }}            as value_usd
    ,event_json:brands                as brands_for_bid_item
    ,event_json:categories            as category_for_bid_item
    ,{{ clean_strings('event_json:currency::text') }}     as currency
    ,case 
      when event_json:discount::text like '$%' then try_to_decimal(event_json:discount::text, '$9,999.99', 7, 2) -- Some values are dollars like $1.23 and others are cents like 123
      else round(event_json:discount::float / 100.0, 2)
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
      else round(event_json:shipping::float / 100.0, 2)
     end as total_shipping_usd
    ,event_json:suggested_add_ons         as suggested_add_ons
    ,{{ cents_to_usd('event_json:tax') }} as tax_usd
    ,case 
      when event_json:total::text like '$%' then try_to_decimal(event_json:total::text, '$9,999.99', 7, 2) -- Some values are dollars like $1.23 and others are cents like 123
      else round(event_json:total::float / 100.0, 2)
     end as total_usd
  from 
    base
  where 
    event_name = 'order_complete'

)

select * from event_order_complete
