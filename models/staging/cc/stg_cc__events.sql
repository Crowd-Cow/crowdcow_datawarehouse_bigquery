{{
  config(
    materialized = 'incremental',
    snowflake_warehouse = 'TRANSFORMING_M',
    unique_key = 'event_id'
    )
}}

with 

events as (
  select
      event_id
      ,visit_id
      ,user_id
      ,occurred_at_utc
      ,updated_at_utc
      ,event_sequence_number
      ,case
          when event_name = 'custom_event' then event_json:category::text || '_' || event_json:action::text
          else replace(event_name,' ','_')
      end as event_name
      ,event_json:context:page:url::text as on_page_url
      ,event_json:context:page:path::text as on_page_path
      ,event_json:properties:url::text as next_page_url
      ,event_json:category::text as category
      ,event_json:action::text as action
      ,coalesce(event_json:label::text,event_json:properties:name::text) as label
      ,event_json:experiments as experiments
      ,event_json:member::boolean as is_member
      ,event_json:properties:id::text as event_properties_id
      ,lower(coalesce(event_json:product_id::text,event_json:properties:product_token::text)) as product_token
      ,event_json:bid_item_id::int as bid_item_id
      ,event_json:"$event_id"::text as token
      ,coalesce(event_json:name::text,event_json:properties:name::text) as name
      ,event_json:order_id::text as order_id
      ,event_json:url::text as url
      ,event_json:referrer_url::text as referrer_url
      ,event_json:subscription_id::text as subscription_id
      ,event_json:title::text as title
      ,coalesce(try_cast(event_json:price::text as int),event_json:properties:price*100::int) as price
      ,event_json:quantity::int as quantity
      ,event_json:old_scheduled_arrival_date::timestamp as old_scheduled_arrival_date
      ,event_json:new_scheduled_arrival_date::timestamp as new_scheduled_arrival_date
      ,event_json:old_scheduled_fulfillment_date::timestamp as old_scheduled_fulfillment_date
      ,event_json:new_scheduled_fulfillment_date::timestamp as new_scheduled_fulfillment_date
      ,event_json:reason::text as reason
      ,event_json:user_making_change_id::int as user_making_change_id
      ,event_json:id::text as brightback_id
      ,event_json:app_id::text as app_id
      ,event_json:fields:session_id::text as session_id
      ,event_json:fields:session_key::text as session_key
      ,event_json:fields:"cancel.account.internal_id"::text as user_token
      ,event_json:fields:"cancel.custom.subscription.token"::text as subscription_token
      ,event_json:survey:display_reason::text as display_reason
      ,event_json:survey:feedback::text as feedback
      ,event_json:survey:selected_reason::text as selected_reason
      ,event_json:properties:depth::int as scroll_depth
      ,event_json:properties:from as from_filter
      ,event_json:properties:to as to_filter
      ,event_json:survey:sentiment::int as sentiment
      ,event_json
  from {{ ref('base_cc__ahoy_events') }}

  {% if is_incremental() %}
    where  updated_at_utc >= coalesce((select max(updated_at_utc) from {{ this }}), '1900-01-01')
  {% endif %}
)

,clean_strings as (
  select 
    event_id
    ,visit_id
    ,user_id
    ,occurred_at_utc
    ,updated_at_utc
    ,event_sequence_number
    ,{{ clean_strings('event_name') }} as event_name
    ,{{ clean_strings('on_page_url') }} as on_page_url
    ,{{ clean_strings('on_page_path') }} as on_page_path
    ,{{ clean_strings('next_page_url') }} as next_page_url
    ,{{ clean_strings('category') }} as category
    ,{{ clean_strings('action') }} as action
    ,{{ clean_strings('label') }} as label
    ,experiments
    ,is_member
    ,{{ clean_strings('event_properties_id') }} as event_properties_id
    ,product_token
    ,bid_item_id
    ,trim(token) as token
    ,{{ clean_strings('name') }} as name
    ,{{ clean_strings('order_id') }} as order_id
    ,{{ clean_strings('url') }} as url
    ,{{ clean_strings('referrer_url') }} as referrer_url
    ,{{ clean_strings('subscription_id') }} as subscription_id
    ,{{ clean_strings('title') }} as title
    ,{{ cents_to_usd('price') }} as price
    ,quantity
    ,old_scheduled_arrival_date
    ,new_scheduled_arrival_date
    ,old_scheduled_fulfillment_date
    ,new_scheduled_fulfillment_date
    ,{{ clean_strings('reason') }} as reason
    ,user_making_change_id
    ,brightback_id
    ,app_id
    ,session_id
    ,session_key
    ,user_token
    ,subscription_token
    ,{{ clean_strings('display_reason') }} as display_reason
    ,{{ clean_strings('feedback') }} as feedback
    ,{{ clean_strings('selected_reason') }} as selected_reason
    ,scroll_depth
    ,from_filter
    ,to_filter
    ,sentiment
  from events
)

select * from clean_strings
