/*** Snowflake prototype dashboard: https://app.snowflake.com/us-east-1/lna65058/first-box-dashboard-includes-any-visitor-that-saw-the-homepage-with-fbq-dZ6KnqQC4 ****/
/**** Total Bundle Volume in Completed Orders Through FBQ - No Customization ****/

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
        ,case
            when dim_user.email like 'TEMPORARY%CROWDCOW.COM%' then TRUE
            when dim_user.email is null then FALSE
            else FALSE
         end as is_guest_user
        ,members.user_id is not null as is_member
        ,date(convert_timezone('UTC','America/Los_Angeles',fact_visit.visited_at)) as visit_date
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
        and (members.user_id is null or members.first_subscription_date >= fact_visit.visited_at)
)

,clicked_on_bundle as (
    select
        fbq_flow.visit_id
        ,count(event_quiz_nav_purchase_bundle.visit_id) as cnt
    from fbq_flow
        inner join event_quiz_nav_purchase_bundle on fbq_flow.visit_id = event_quiz_nav_purchase_bundle.visit_id
    group by 1
)


,clicked_customize_bundle as (
    select
        fbq_flow.visit_id
        ,count(event_quiz_nav_customize_bundle.visit_id) as cnt
    from fbq_flow
        inner join event_quiz_nav_customize_bundle on fbq_flow.visit_id = event_quiz_nav_customize_bundle.visit_id
    group by 1
)

,bid_items_of_bundles as (
    select 
        event_quiz_nav_purchase_bundle.bid_item_key
    from fbq_flow
        inner join event_quiz_nav_purchase_bundle on event_quiz_nav_purchase_bundle.visit_id = fbq_flow.visit_id
)

select dim_bid_item.bid_item_name
, fbq_flow.visit_date
, count(distinct fact_order_item.order_id) as completed_order_volume
from fbq_flow
inner join fact_order on fact_order.visit_id = fbq_flow.visit_id
inner join fact_order_item on fact_order_item.order_id = fact_order.order_id
inner join bid_items_of_bundles on bid_items_of_bundles.bid_item_key = fact_order_item.bid_item_key
inner join dim_bid_item on dim_bid_item.bid_item_key = fact_order_item.bid_item_key
where fact_order.checkout_completed_at is not null 
group by 1, 2
