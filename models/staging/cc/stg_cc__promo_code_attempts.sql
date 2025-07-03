{{
  config(
    materialized='incremental',
    unique_key='id',
    partition_by={
      "field": "created_at_utc",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by = ["user_id", "order_token", "promo_code"]
  )
}}

with
domain_events as (

  select * from {{ source('cc', 'domain_events') }}

  where event_type = 'promo_code_used'
  
  {% if is_incremental() %}
    -- This filter ensures we only scan events that are new since the last run.
    and created_at > (select max(created_at_utc) from {{ this }})
  {% endif %}

),

attempts as (
  select
    created_at as created_at_utc
    ,entity_id
    ,event_type
    ,id
    ,user_id
    ,JSON_EXTRACT_SCALAR(data, '$.user_token') AS user_token
    ,JSON_EXTRACT_SCALAR(data, '$.order_token') AS order_token
    ,JSON_EXTRACT_SCALAR(data, '$.result') AS result
    ,JSON_EXTRACT_SCALAR(data, '$.error_report') AS error_report
    ,JSON_EXTRACT_SCALAR(data, '$.code') AS code
  from domain_events

  -- The QUALIFY clause is now much faster because it only runs on the small,
  -- incremental batch of new events, not the entire table history.
  qualify row_number() over(partition by user_id, order_token order by created_at desc, id desc) = 1
)

select
    created_at_utc
    ,entity_id
    ,event_type
    ,id
    ,user_id
    ,user_token
    ,order_token
    ,result
    ,error_report
    ,{{ clean_strings('code') }} as promo_code
from attempts