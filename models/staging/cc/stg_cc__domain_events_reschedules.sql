{{
  config(
    materialized='incremental',
    unique_key='id',
    partition_by={
      "field": "created_at_utc",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by = ["user_id", "order_id"]
  )
}}

with
domain_events as (

  select * from {{ source('cc', 'domain_events') }}

  where event_type = 'order_rescheduled'
  
  {% if is_incremental() %}
    -- This filter ensures we only scan events that are new since the last run.
    and created_at > (select max(created_at_utc) from {{ this }})
  {% endif %}

),

reschedules as (
  select
    created_at as created_at_utc
    ,entity_id as order_id
    ,event_type
    ,id
    ,user_id
    ,SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.new_scheduled_arrival_date') as timestamp) AS new_scheduled_arrival_date
    ,SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.new_scheduled_fulfillment_date') as timestamp) AS new_scheduled_fulfillment_date
    ,SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.old_scheduled_arrival_date') as timestamp) AS old_scheduled_arrival_date
    ,SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.old_scheduled_fulfillment_date') as timestamp) AS old_scheduled_fulfillment_date
    ,JSON_EXTRACT_SCALAR(data, '$.reason') AS reason
    ,JSON_EXTRACT_SCALAR(data, '$.shipping_description') AS shipping_description
    ,row_number() over(partition by entity_id order by created_at desc, id desc) as event_sequence_number
  from domain_events

  -- The QUALIFY clause is now much faster because it only runs on the small,
  -- incremental batch of new events, not the entire table history.
  
)

select
    created_at_utc
    ,order_id
    ,event_type
    ,id
    ,user_id
    ,new_scheduled_arrival_date
    ,new_scheduled_fulfillment_date
    ,old_scheduled_arrival_date
    ,old_scheduled_fulfillment_date
    ,{{ clean_strings('reason') }} as reason
    ,{{ clean_strings('shipping_description') }} as shipping_description
    ,event_sequence_number
from reschedules