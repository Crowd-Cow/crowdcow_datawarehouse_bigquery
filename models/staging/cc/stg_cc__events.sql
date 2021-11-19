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
      ,row_number() over(partition by visit_id order by occurred_at_utc, event_id) as event_sequence_number
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
    ,{{ clean_strings('token') }} as token
    ,{{ clean_strings('order_id') }} as order_id
    ,{{ clean_strings('url') }} as url
    ,{{ clean_strings('referrer_url') }} as referrer_url
    ,{{ clean_strings('subscription_id') }} as subscription_id
    ,{{ clean_strings('title') }} as title
  from events
)

select * from clean_strings
