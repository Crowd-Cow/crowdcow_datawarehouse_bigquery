{{
  config(
    tags=["base", "events"],
    partition_by = {'field': 'occurred_at_utc', 'data_type': 'timestamp'},
    cluster_by = ['visit_id','user_id','event_name']
  )
}}

with base_ahoy_events as (
  select
     id        as event_id
    ,visit_id
    ,row_number() over(partition by visit_id order by time, id) as event_sequence_number
    ,time      as occurred_at_utc
    ,updated_at as updated_at_utc
    ,user_id
    ,properties as event_json
    ,CASE
       WHEN name = 'custom_event' THEN CONCAT(SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.category') AS STRING), '_', SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.action') AS STRING))
       ELSE REPLACE(name, ' ', '_')
    END AS event_name,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.context.page.url') AS STRING) AS on_page_url,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.context.page.path') AS STRING) AS on_page_path,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.properties.url') AS STRING) AS next_page_url,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.category') AS STRING) AS category,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.action') AS STRING) AS action,
    COALESCE(SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.label') AS STRING), SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.properties.name') AS STRING)) AS label,
    JSON_EXTRACT(properties, '$.experiments') AS experiments,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.member') AS BOOL) AS is_member,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.properties.id') AS STRING) AS event_properties_id,
    LOWER(COALESCE(SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.product_id') AS STRING), SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.properties.product_token') AS STRING))) AS product_token,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.bid_item_id') AS INT64) AS bid_item_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.event_id') AS STRING) AS token,
    COALESCE(SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.name') AS STRING), SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.properties.name') AS STRING)) AS name,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.properties.page_section') AS STRING) AS page_section,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.properties.modal_name') AS STRING) AS modal_name,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.order_id') AS STRING) AS order_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.url') AS STRING) AS url,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.referrer_url') AS STRING) AS referrer_url,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.subscription_id') AS STRING) AS subscription_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.title') AS STRING) AS title,
    COALESCE(SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.price') AS INT64), SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.properties.price') AS INT64) * 100) AS price,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.quantity') AS INT64) AS quantity,
    TIMESTAMP(SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.old_scheduled_arrival_date') AS STRING)) AS old_scheduled_arrival_date,
    TIMESTAMP(SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.new_scheduled_arrival_date') AS STRING)) AS new_scheduled_arrival_date,
    TIMESTAMP(SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.old_scheduled_fulfillment_date') AS STRING)) AS old_scheduled_fulfillment_date,
    TIMESTAMP(SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.new_scheduled_fulfillment_date') AS STRING)) AS new_scheduled_fulfillment_date,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.reason') AS STRING) AS reason,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.user_making_change_id') AS INT64) AS user_making_change_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.id') AS STRING) AS brightback_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.app_id') AS STRING) AS app_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.fields.session_id') AS STRING) AS session_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.fields.session_key') AS STRING) AS session_key,
    JSON_EXTRACT_SCALAR(properties, '$.fields.cancel.account.internal_id') AS user_token,
    JSON_EXTRACT_SCALAR(properties, '$.fields.cancel.custom.subscription.token') AS subscription_token,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.survey.display_reason') AS STRING) AS display_reason,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.survey.feedback') AS STRING) AS feedback,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.survey.selected_reason') AS STRING) AS selected_reason,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.properties.depth') AS INT64) AS scroll_depth,
    JSON_EXTRACT(properties, '$.properties.from') AS from_filter,
    JSON_EXTRACT(properties, '$.properties.to') AS to_filter,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.survey.sentiment') AS INT64) AS sentiment,
    SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.product_offer.quantity_sellable') AS INT64) AS quantity_sellable,
    COALESCE(SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.value') AS STRING), SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.properties.value') AS STRING)) AS event_value
  from
    {{ source('cc', 'ahoy_events') }}
  
)

select * from base_ahoy_events
