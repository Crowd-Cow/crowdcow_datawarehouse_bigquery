with

visits as ( select * from {{ ref('base_cc__ahoy_visits') }} ),
orders as ( select * from {{ ref('stg_cc__orders') }} ),

daily_ip_visits as (
    select 
        visits.visit_ip
        ,date(visits.started_at_utc) as visit_date
        ,count(visits.visit_id) as daily_visit_count
    from visits
        left join orders on visits.visit_id = orders.visit_id
    where orders.visit_id is null
        and DATE(visits.started_at_utc) >= DATE_ADD(CURRENT_DATE(), INTERVAL -10 DAY)
        and visits.visit_ip is not null
    group by 1,2
),

average_daily_visits as (
    select
        visit_ip
        ,sum(daily_visit_count)/10 as avg_daily_visits
    from daily_ip_visits
    group by 1
),

suspicious_ips as (
    select 
        *
    from average_daily_visits
    where avg_daily_visits > 1000
)

select * from suspicious_ips
