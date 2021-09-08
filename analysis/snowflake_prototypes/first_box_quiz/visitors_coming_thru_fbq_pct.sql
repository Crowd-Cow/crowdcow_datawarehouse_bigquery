/*** Snowflake prototype dashboard: https://app.snowflake.com/us-east-1/lna65058/first-box-dashboard-includes-any-visitor-that-saw-the-homepage-with-fbq-dZ6KnqQC4 ****/with

invalid_experiments as (
       select
        visit_id
        ,experiment_token
        ,count(distinct experiment_variant) as variant_count
    from experiments_by_event_id
    where experiment_token = :experiment_token
    group by 1,2
    having count(distinct experiment_variant) > 1
),

valid_experiments as (
    select distinct
        visit_id
        ,experiment_token
        ,experiment_variant
    from experiments_by_event_id
    where visit_id not in (select visit_id from invalid_experiments)
        and experiment_token = :experiment_token
),

members as (
    select
        user_id
        ,min(created_at) as first_subscription_date
        ,count(subscription_id) as subscription_count
    from dim_subscription
    where dbt_valid_to is null
    group by 1
)

,traffic_to_fbq as (
    select
        visit_id
        ,count(event_id) as event_count
    from fact_event_pageview
    where parse_url(url):path::text = 'first-box'
        and occurred_at >= '2021-08-25'
    group by 1
)

,fbq_flow as (
    select
        fact_visit.visit_id
        ,fact_visit.user_id
        ,fact_visit.visitor_token
        ,concat(fact_visit.visitor_token, '-', date(convert_timezone('UTC','America/Los_Angeles',fact_visit.visited_at)))as visitor_session
        ,case
            when dim_user.email like 'TEMPORARY%CROWDCOW.COM%' then TRUE
            when dim_user.email is null then FALSE
            else FALSE
         end as is_guest_user
        ,members.user_id is not null as is_member
        ,date(convert_timezone('UTC','America/Los_Angeles',fact_visit.visited_at)) as visit_date
        ,convert_timezone('UTC','America/Los_Angeles',fact_visit.visited_at) as visited_at
        ,fact_visit.device_type as device_type
        ,valid_experiments.experiment_token
        ,valid_experiments.experiment_variant
    from fact_visit
        inner join valid_experiments on fact_visit.visit_id = valid_experiments.visit_id
        left join dim_user on fact_visit.user_id = dim_user.user_id
            and dim_user.dbt_valid_to is null
        left join members on fact_visit.user_id = members.user_id
    where convert_timezone('UTC','America/Los_Angeles',fact_visit.visited_at) = :daterange
        and fact_visit.visit_id in (select visit_id from traffic_to_fbq)
        and not fact_visit.is_bot
        and not fact_visit.is_internal_traffic
        and (utm_medium <> 'FIELD-MARKETING' or utm_medium is null)
        and valid_experiments.experiment_variant = 'experimental'
        and (members.user_id is null or members.first_subscription_date >= fact_visit.visited_at)
)

, aggregates as (
select fbq_flow.experiment_variant
, count(distinct fbq_flow.visitor_token) as distinct_visitors
, count( distinct case when fact_order.checkout_completed_at >= fbq_flow.visited_at  
                        and fact_order.checkout_completed_at is not null 
                    then fact_order.order_id else null end ) as completed_orders

, count(distinct case when fact_order.checkout_completed_at >= fbq_flow.visited_at  
                        and fact_order.checkout_completed_at is not null 
                        and fact_order.is_subscription = true
                    then fact_order.order_id else null end) as completed_first_member_orders 
, count( distinct case when fact_order.checkout_completed_at >= fbq_flow.visited_at  
                        and fact_order.checkout_completed_at is not null 
                        and fact_order.is_subscription = false
                    then fact_order.order_id else null end) as completed_non_member_orders 
//, round(
//    avg( coalesce(fact_order_paid_uncancelled.product_revenue,0) + coalesce(fact_order_paid_uncancelled.shipping_revenue,0) -
//        coalesce(fact_order_paid_uncancelled.disputes,0)  - coalesce(fact_order_paid_uncancelled.discounts,0) - coalesce(fact_order_paid_uncancelled.refunds,0)),2)
//            as net_aov_paid_uncancelled
   ,round(avg(case when fact_order.checkout_completed_at is not null then fact_order.product_revenue + fact_order.shipping_revenue - fact_order.disputes  - fact_order.discounts - fact_order.refunds else null end),2)
        as completed_orders_net_aov
, count( distinct case when fact_order.ordered_at >= fbq_flow.visited_at  
                        and fact_order.ordered_at is not null  
                        and fact_order.is_subscription = true
                        and fact_order.cancelled_at is null 
                        and fact_order.order_rank_alc_sub = 1
                    then fact_order.order_id else null end) as paid_uncancelled_first_member_orders

                 
                        

from fbq_flow
        left join fact_order on fact_order.visit_id = fbq_flow.visit_id
        left join (select product_revenue,shipping_revenue,discounts,disputes,refunds,ordered_at,cancelled_at,order_id,visit_id 
                    from fact_order
                    where fact_order.cancelled_at is null 
                    and fact_order.ordered_at is not null ) as fact_order_paid_uncancelled on fact_order_paid_uncancelled.visit_id = fbq_flow.visit_id
group by 1
order by 2 desc ) 

select 
    distinct_visitors as "Distinct Visitors"
    -- ,completed_orders as "Completed Orders"
    -- ,completed_first_member_orders as "Completed First Member Orders"
    -- ,completed_non_member_orders as "Completed Non Member Orders"
    -- ,to_char(completed_orders_net_aov,'$9999.00') as "Completed Orders Net AOV"
    -- ,paid_uncancelled_first_member_orders as "Paid Uncancelled First Member Orders"
    ,completed_orders::float / distinct_visitors as "Completed Order Rate"
    ,completed_first_member_orders::float / distinct_visitors as "Completed Member Order Rate"
    ,completed_non_member_orders::float / distinct_visitors as "Non Member Completed Order Rate"
    ,paid_uncancelled_first_member_orders::float / distinct_visitors as "First Member Paid Order Rate"
from aggregates;
