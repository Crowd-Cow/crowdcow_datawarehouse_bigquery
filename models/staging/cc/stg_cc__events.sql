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
          when event_name = 'custom_event' then event_json:action::text
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
  from {{ ref('base_cc__ahoy_events') }}

  {% if is_incremental() %}
    where  updated_at_utc >= coalesce((select max(updated_at_utc) from {{ this }}), '1900-01-01')
  {% endif %}
)

select * from events