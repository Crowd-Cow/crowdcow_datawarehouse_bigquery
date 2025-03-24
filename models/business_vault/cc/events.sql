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
    select * 
    from {{ ref('stg_cc__events') }}   

    {% if is_incremental() %}
      where timestamp_trunc(occurred_at_utc, day) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
)

,event_details as (
    select event_id
        ,visit_id
        ,user_id
        ,occurred_at_utc
        ,updated_at_utc
        ,event_sequence_number
        ,event_name
        ,on_page_url
        ,on_page_path
        ,next_page_url
        ,category
        ,action
        ,label
        ,page_section
        ,modal_name
        ,experiments
        ,is_member
        ,event_properties_id
        ,product_token
        ,bid_item_id
        ,token
        ,name
        ,order_id
        ,url
        ,referrer_url
        ,subscription_id
        ,title
        ,price
        ,quantity
        ,old_scheduled_arrival_date
        ,new_scheduled_arrival_date
        ,old_scheduled_fulfillment_date
        ,new_scheduled_fulfillment_date
        ,reason
        ,user_making_change_id
        ,brightback_id
        ,app_id
        ,session_id
        ,session_key
        ,user_token
        ,subscription_token
        ,display_reason
        ,feedback
        ,selected_reason
        ,scroll_depth
        ,from_filter
        ,to_filter
        ,sentiment
        ,quantity_sellable
        ,event_value
        ,pdc_in_stock
        ,pdp_in_stock
    from events
)


select *
from event_details
