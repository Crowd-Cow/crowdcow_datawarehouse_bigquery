{{
  config(
    tags=["events"]
  )
}}

with base as (
  
  select * from {{ ref('base_cc__ahoy_events') }}

),

event_viewed_product as (

  select
     event_id
    ,visit_id
    ,occurred_at_utc
    ,user_id
    ,event_json:experiments       as experiments
    ,event_json:member::boolean   as is_member
    ,event_json:brand             as brands_for_bid_item
    ,event_json:category          as category_for_bid_item
    ,event_json:image_url::text   as bid_item_image_url
    ,event_json:title::text       as bid_item_name
    ,event_json:url::text         as product_page_url
  from 
    base
  where 
    event_name = 'viewed_product'

)

select * from event_viewed_product
