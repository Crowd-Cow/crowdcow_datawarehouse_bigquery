with

reschedule as ( select * from {{ ref('events') }} where event_name = 'ORDER_RESCHEDULED')

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
