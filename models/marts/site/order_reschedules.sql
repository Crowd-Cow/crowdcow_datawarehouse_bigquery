{% set partitions_to_replace = [
  'timestamp(current_date)',
  'timestamp(date_sub(current_date, interval 1 day))'
] %}
{{
  config(
        materialized = 'incremental',
        partition_by = {'field': 'occurred_at_utc', 'data_type': 'timestamp'},
        cluster_by = ['event_id','visit_id','user_id','order_id'],
        incremental_strategy = 'insert_overwrite',
        partitions = partitions_to_replace
    )
}}
with

reschedule as ( 
    select 
        event_id
        ,visit_id
        ,user_id
        ,order_id
        ,user_making_change_id
        ,reason
        ,old_scheduled_arrival_date
        ,new_scheduled_arrival_date
        ,old_scheduled_fulfillment_date
        ,new_scheduled_fulfillment_date
        ,occurred_at_utc
        ,token
    from {{ ref('events') }} 
    where event_name = 'ORDER_RESCHEDULED'
    {% if is_incremental() %}
     and timestamp_trunc(occurred_at_utc, day) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
    )

,transform_event as (
    select
        event_id
        ,visit_id
        ,user_id
        ,order_id
        ,user_making_change_id

        ,case
            when reason like '%RENEW PERIOD%' then 'RENEWAL_PERIOD_CHANGE'
            when reason like '%NORMAL_OVERRIDE%' then 'ORDER_STATE_NORMAL_OVERRIDE'
            when reason like '%SUBSCRIPTIONBOT%' then 'SUBSCRIPTIONBOT_RESCHEDULE'
            when reason like '%UPDATE_ARRIVAL_DATE%' then 'UPDATE_ARRIVAL_DATE'
            else reason
         end as reason
    
        ,old_scheduled_fulfillment_date < new_scheduled_fulfillment_date as is_pushed_order
        ,user_making_change_id = user_id and user_making_change_id is not null as is_customer_reschedule
        ,old_scheduled_arrival_date
        ,new_scheduled_arrival_date
        ,old_scheduled_fulfillment_date
        ,new_scheduled_fulfillment_date
        ,occurred_at_utc
        ,upper(token) as token
    from reschedule
)

select * from transform_event
