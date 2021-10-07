/*** Snowflake prototype dashboard: https://app.snowflake.com/us-east-1/lna65058/first-box-dashboard-includes-any-visitor-that-saw-the-homepage-with-fbq-dZ6KnqQC4 ****/
/**** Daily Purchase by Variant Rates (by Visitor Token) | Experimental and Control ****/

with

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

,homepage_view_events as (
    select
        visit_id
        ,count(event_id) as event_count
    from fact_event_pageview
    where (parse_url(url):path::text = '' or parse_url(url):path::text = 'l')
        and url not like '%/?first-box%'
    group by 1
)


, homepage_fbq_impressions as (
    select
        fact_visit.visit_id
        ,fact_visit.user_id
        ,fact_visit.visitor_token
        , concat(fact_visit.visitor_token, '-', date(convert_timezone('UTC','America/Los_Angeles',fact_visit.visited_at)))as visitor_session
        ,case
            when dim_user.email like 'TEMPORARY%CROWDCOW.COM%' then TRUE
            when dim_user.email is null then FALSE
            else FALSE
         end as is_guest_user
        ,members.user_id is not null as is_member
        ,date(convert_timezone('UTC','America/Los_Angeles',fact_visit.visited_at)) as visit_date
        , convert_timezone('UTC','America/Los_Angeles',fact_visit.visited_at) as visited_at
        ,fact_visit.device_type as device_type
        ,valid_experiments.experiment_token
        ,valid_experiments.experiment_variant
    from fact_visit
        inner join valid_experiments on fact_visit.visit_id = valid_experiments.visit_id
        left join dim_user on fact_visit.user_id = dim_user.user_id
            and dim_user.dbt_valid_to is null
        left join members on fact_visit.user_id = members.user_id
    
    where convert_timezone('UTC','America/Los_Angeles',fact_visit.visited_at) = :daterange
        and fact_visit.visit_id in (select visit_id from homepage_view_events)
        and  fact_visit.is_bot = false
        and  fact_visit.is_internal_traffic = false
      --   and valid_experiments.experiment_variant = 'experimental'
        and (utm_medium != 'FIELD-MARKETING' or utm_medium is null )
        and (members.user_id is null or members.first_subscription_date >= fact_visit.visited_at)
)

,clicked_fbq_cta as (
    select
        homepage_fbq_impressions.*
    from homepage_fbq_impressions
        inner join event_first_box_hero_selected on homepage_fbq_impressions.visit_id = event_first_box_hero_selected.visit_id
)

, aggregates as (
select homepage_fbq_impressions.visit_date
,experiment_variant
, count(distinct homepage_fbq_impressions.visitor_token) as distinct_visitors
, count( distinct case when fact_order.checkout_completed_at >= homepage_fbq_impressions.visited_at  
                        and fact_order.checkout_completed_at is not null 
                    then fact_order.order_id else null end ) as completed_orders

, count(distinct case when fact_order.checkout_completed_at >= homepage_fbq_impressions.visited_at  
                        and fact_order.checkout_completed_at is not null 
                        and fact_order.is_subscription = true
                    then fact_order.order_id else null end) as completed_first_member_orders 
, count( distinct case when fact_order.checkout_completed_at >= homepage_fbq_impressions.visited_at  
                        and fact_order.checkout_completed_at is not null 
                        and fact_order.is_subscription = false
                    then fact_order.order_id else null end) as completed_non_member_orders 
//, round(
//    avg( coalesce(fact_order_paid_uncancelled.product_revenue,0) + coalesce(fact_order_paid_uncancelled.shipping_revenue,0) -
//        coalesce(fact_order_paid_uncancelled.disputes,0)  - coalesce(fact_order_paid_uncancelled.discounts,0) - coalesce(fact_order_paid_uncancelled.refunds,0)),2)
//            as net_aov_paid_uncancelled
   ,round(avg(case when fact_order.checkout_completed_at is not null then fact_order.product_revenue + fact_order.shipping_revenue - fact_order.disputes  - fact_order.discounts - fact_order.refunds else null end),2)
        as completed_orders_net_aov
, count( distinct case when fact_order.ordered_at >= homepage_fbq_impressions.visited_at  
                        and fact_order.ordered_at is not null  
                        and fact_order.is_subscription = true
                        and fact_order.cancelled_at is null 
                        and fact_order.order_rank = 1
                    then fact_order.order_id else null end) as paid_uncancelled_first_member_orders

                 
                        

from homepage_fbq_impressions
left join fact_order on fact_order.visit_id = homepage_fbq_impressions.visit_id
left join (select product_revenue,shipping_revenue,discounts,disputes,refunds,ordered_at,cancelled_at,order_id,visit_id 
            from fact_order
            where fact_order.cancelled_at is null 
            and fact_order.ordered_at is not null ) as fact_order_paid_uncancelled on fact_order_paid_uncancelled.visit_id = homepage_fbq_impressions.visit_id
group by 1, 2
order by 1 desc, 2 asc ) 

select
    visit_date as "Visit Date"
    ,experiment_variant as "Experiment Variant"
    -- ,distinct_visitors as "Distinct Visitors"
    -- ,completed_orders as "Completed Orders"
    -- ,completed_first_member_orders as "Completed First Member Orders"
    -- ,completed_non_member_orders as "Completed Non Member Orders"
    -- ,to_char(completed_orders_net_aov,'$9999.00') as "Completed Orders Net AOV"
    -- ,paid_uncancelled_first_member_orders as "Paid Uncancelled First Member Orders"
    ,completed_orders::float / distinct_visitors as "Completed Order Rate Pct"
    ,completed_first_member_orders::float / distinct_visitors as "Completed Member Order Rate Pct"
    ,completed_non_member_orders::float / distinct_visitors as "Non Member Completed Order Rate Pct"
    ,paid_uncancelled_first_member_orders::float / distinct_visitors as "First Member Paid Order Rate Pct"
from aggregates
order by 1 desc, 2 desc;
