{% set partitions_to_replace = [
  'timestamp(current_date)',
  'timestamp(date_sub(current_date, interval 1 day))'
] %}
{{
  config(
        materialized = 'incremental',
        partition_by = {'field': 'occurred_at_utc', 'data_type': 'timestamp'},
        cluster_by = ['order_id'],
        incremental_strategy = 'insert_overwrite',
        partitions = partitions_to_replace
    )
}}
with

order_reschedule as ( 
    select 
        order_id
        ,occurred_at_utc
        ,reason
        ,old_scheduled_fulfillment_date
        ,new_scheduled_fulfillment_date
        ,user_making_change_id
        ,user_id 
        ,event_id
    from {{ ref('stg_cc__events') }} 
    where event_name = 'ORDER_RESCHEDULED'
    {% if is_incremental() %}
     and timestamp_trunc(occurred_at_utc, day) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
    )

select
    order_id
    ,occurred_at_utc
    ,reason as reschedule_reason
    ,old_scheduled_fulfillment_date
    ,new_scheduled_fulfillment_date
    ,user_making_change_id = user_id and user_making_change_id is not null as is_customer_reschedule
    ,count(event_id) over(partition by order_id) as reschedule_count
from order_reschedule
qualify row_number() over(partition by order_id order by occurred_at_utc desc) = 1
