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
    partitions = partitions_to_replace
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
  from {{ ref('base_cc__ahoy_events') }}

  {% if is_incremental() %}
    where timestamp_trunc(occurred_at_utc, day) in ({{ partitions_to_replace | join(',') }})
  {% endif %}
)

select * from events
