{{
  config(
    materialized = 'incremental',
    unique_key = 'event_id',
    tags=["events"]
  )
}}
    
with base as (
  
  select * 
  from {{ ref('base_cc__ahoy_events') }} as ae
  where event_name = 'custom_event'
      and event_json:category::text = 'product'
      and event_json:action::text = 'page-interaction' 
      and event_json:label::text = 'clicked-add-to-cart' 

    {% if is_incremental() %}
      and ae.occurred_at_utc >= coalesce((select max(occurred_at_utc) from {{ this }}), '1900-01-01')
    {% endif %}
    
),

event_pdp_clicked_add_to_cart as (

  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:member::boolean as is_member
  from 
    base

)

select * from event_pdp_clicked_add_to_cart
