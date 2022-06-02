{{
  config(
        materialized = 'incremental',
        unique_key = 'event_id',
        snowflake_warehouse = 'TRANSFORMING_M'
    )
}}


with

events as ( 
    select * 
    from {{ ref('stg_cc__events') }}   

    {% if is_incremental() %}
      where occurred_at_utc >= coalesce((select max(occurred_at_utc) from {{ this }}), '1900-01-01')
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
    from events
)


select *
from event_details
