{% set partitions_to_replace = [
  'timestamp(current_date)',
  'timestamp(date_sub(current_date, interval 1 day))'
] %}
{{
  config(
        materialized = 'incremental',
        partition_by = {'field': 'occurred_at_utc', 'data_type': 'timestamp'},
        cluster_by = ['visit_id','user_id','event_name'],
        incremental_strategy = 'insert_overwrite',
        partitions = partitions_to_replace,
        on_schema_change = 'sync_all_columns'
    )
}}

with 

events as (
  select
   event_id,
   visit_id,
   user_id,
   occurred_at_utc,
   updated_at_utc,
   event_sequence_number,
   CASE
       WHEN event_name = 'custom_event' THEN CONCAT(SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.category') AS STRING), '_', SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.action') AS STRING))
       ELSE REPLACE(event_name, ' ', '_')
   END AS event_name,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.context.page.url') AS STRING) AS on_page_url,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.context.page.path') AS STRING) AS on_page_path,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.properties.url') AS STRING) AS next_page_url,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.category') AS STRING) AS category,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.action') AS STRING) AS action,
   COALESCE(SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.label') AS STRING), SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.properties.name') AS STRING)) AS label,
   JSON_EXTRACT(event_json, '$.experiments') AS experiments,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.member') AS BOOL) AS is_member,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.properties.id') AS STRING) AS event_properties_id,
   LOWER(COALESCE(SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.product_id') AS STRING), SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.properties.product_token') AS STRING))) AS product_token,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.bid_item_id') AS INT64) AS bid_item_id,
   SAFE_CAST(JSON_VALUE(event_json, '$."$event_id"') AS STRING) AS token,
   COALESCE(SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.name') AS STRING), SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.properties.name') AS STRING)) AS name,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.properties.page_section') AS STRING) AS page_section,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.properties.modal_name') AS STRING) AS modal_name,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.order_id') AS STRING) AS order_id,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.url') AS STRING) AS url,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.referrer_url') AS STRING) AS referrer_url,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.subscription_id') AS STRING) AS subscription_id,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.title') AS STRING) AS title,
   COALESCE(SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.price') AS INT64), SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.properties.price') AS INT64) * 100) AS price,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.quantity') AS INT64) AS quantity,
   TIMESTAMP(SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.old_scheduled_arrival_date') AS STRING)) AS old_scheduled_arrival_date,
   TIMESTAMP(SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.new_scheduled_arrival_date') AS STRING)) AS new_scheduled_arrival_date,
   TIMESTAMP(SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.old_scheduled_fulfillment_date') AS STRING)) AS old_scheduled_fulfillment_date,
   TIMESTAMP(SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.new_scheduled_fulfillment_date') AS STRING)) AS new_scheduled_fulfillment_date,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.reason') AS STRING) AS reason,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.user_making_change_id') AS INT64) AS user_making_change_id,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.id') AS STRING) AS brightback_id,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.app_id') AS STRING) AS app_id,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.fields.session_id') AS STRING) AS session_id,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.fields.session_key') AS STRING) AS session_key,
   JSON_EXTRACT_SCALAR(event_json, '$.fields.cancel.account.internal_id') AS user_token,
   JSON_EXTRACT_SCALAR(event_json, '$.fields.cancel.custom.subscription.token') AS subscription_token,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.survey.display_reason') AS STRING) AS display_reason,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.survey.feedback') AS STRING) AS feedback,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.survey.selected_reason') AS STRING) AS selected_reason,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.properties.depth') AS INT64) AS scroll_depth,
   JSON_EXTRACT(event_json, '$.properties.from') AS from_filter,
   JSON_EXTRACT(event_json, '$.properties.to') AS to_filter,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.survey.sentiment') AS INT64) AS sentiment,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.product_offer.quantity_sellable') AS INT64) AS quantity_sellable,
   COALESCE(SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.value') AS STRING), SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.properties.value') AS STRING)) AS event_value,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.properties.in_stock') as BOOL) as pdc_in_stock,
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.product_offer.in_stock') as BOOL) as pdp_in_stock,
   coalesce(JSON_VALUE(event_json, '$.brands[0]'),JSON_EXTRACT_SCALAR(event_json, '$.properties.brand'))  AS brands,
   JSON_VALUE(event_json, '$.categories[0]') AS categories
  from {{ ref('base_cc__ahoy_events') }}

  {% if is_incremental() %}
     where timestamp_trunc(occurred_at_utc, day) in ({{ partitions_to_replace | join(',') }})
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
    ,{{ clean_strings('page_section') }} as page_section
    ,{{ clean_strings('modal_name') }} as modal_name
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
    ,quantity_sellable
    ,{{ clean_strings('event_value') }} as event_value
    ,pdc_in_stock
    ,pdp_in_stock
    ,{{ clean_strings('brands') }} as brands
    ,{{ clean_strings('categories') }} as categories
  from events
)

select * from clean_strings
