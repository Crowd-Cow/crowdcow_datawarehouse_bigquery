/*** Snowflake prototype dashboard: https://app.snowflake.com/us-east-1/lna65058/first-box-dashboard-includes-any-visitor-that-saw-the-homepage-with-fbq-dZ6KnqQC4 ****/
/**** Prospect to First Completed Order by Variant ****/

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
        and (utm_medium != 'FIELD-MARKETING' or utm_medium is null )
        and (members.user_id is null or members.first_subscription_date >= fact_visit.visited_at)
)

,clicked_fbq_cta as (
    select
        homepage_fbq_impressions.*
    from homepage_fbq_impressions
        inner join event_first_box_hero_selected on homepage_fbq_impressions.visit_id = event_first_box_hero_selected.visit_id
)


,first_completed as (
    select fo.*, first_completed_at from (select min(checkout_completed_at) as first_completed_at, user_id
                        from fact_order
                        where checkout_completed_at is not null
                        group by 2
) foc

left join fact_order fo on fo.user_id = foc.user_id
and fo.checkout_completed_at = foc.first_completed_at
and checkout_completed_at is not null
)


select 
    experiment_variant
    ,homepage_fbq_impressions.visit_date
     ,count(distinct first_completed.order_id)::float/count(distinct homepage_fbq_impressions.visitor_token)::float as first_completed_conversion_rate
from homepage_fbq_impressions
left join first_completed on first_completed.visit_id = homepage_fbq_impressions.visit_id
group by 1,2
