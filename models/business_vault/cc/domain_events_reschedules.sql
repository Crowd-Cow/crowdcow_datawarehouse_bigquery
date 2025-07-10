with

domain_events_reschedules as ( select * from {{ ref('stg_cc__domain_events_reschedules') }} )
,orders as (select user_id, order_id from {{ ref('orders') }} )


select 
    domain_events_reschedules.*
    ,orders.user_id as order_user_id
    ,case 
        when domain_events_reschedules.user_id is not null and orders.user_id = domain_events_reschedules.user_id then "CUSTOMER RESCHEDULED"
        when domain_events_reschedules.user_id is not null and orders.user_id != domain_events_reschedules.user_id then "CC CARE RESCHEDULED"
        when domain_events_reschedules.user_id is null then "CC SYSTEM RESCHEDULED"
        else null end as reschedule_reason
    from domain_events_reschedules 
    left join orders on orders.order_id = domain_events_reschedules.order_id