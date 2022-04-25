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
          else event_name
      end as event_name
      ,event_json:category::text as category
      ,event_json:action::text as action
      ,event_json:label::text as label
      ,event_json:experiments as experiments
      ,event_json:member::boolean as is_member
      ,event_json:"$event_id"::text as token
      ,event_json:order_id::text as order_id
      ,event_json:url::text as url
      ,event_json:referrer_url::text as referrer_url
      ,event_json:subscription_id::text as subscription_id
      ,event_json:title::text as title
      ,event_json:old_scheduled_arrival_date::timestamp as old_scheduled_arrival_date
      ,event_json:new_scheduled_arrival_date::timestamp as new_scheduled_arrival_date
      ,event_json:old_scheduled_fulfillmet_date::timestamp as old_scheduled_fulfillment_date
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
    ,{{ clean_strings('category') }} as category
    ,{{ clean_strings('action') }} as action
    ,{{ clean_strings('label') }} as label
    ,experiments
    ,is_member
    ,trim(token) as token
    ,{{ clean_strings('order_id') }} as order_id
    ,{{ clean_strings('url') }} as url
    ,{{ clean_strings('referrer_url') }} as referrer_url
    ,{{ clean_strings('subscription_id') }} as subscription_id
    ,{{ clean_strings('title') }} as title
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
    ,subscription_token
    ,{{ clean_strings('display_reason') }} as display_reason
    ,{{ clean_strings('feedback') }} as feedback
    ,{{ clean_strings('selected_reason') }} as selected_reason
  from events
)

select * from clean_strings
