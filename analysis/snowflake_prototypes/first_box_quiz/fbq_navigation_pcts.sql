/*** Snowflake prototype dashboard: https://app.snowflake.com/us-east-1/lna65058/first-box-dashboard-includes-any-visitor-that-saw-the-homepage-with-fbq-dZ6KnqQC4 ****/
/**** FBQ Navigation Percents | Number of Clicks on FBQ Navigation Elements / Number of FBQ CTA Clicks ****/

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
    select distinctwith

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

,clicked_nav_back as (
    select
        fbq_flow.visit_id
        ,count(event_quiz_nav_back_clicked.visit_id) as cnt
    from fbq_flow
        inner join event_quiz_nav_back_clicked on fbq_flow.visit_id = event_quiz_nav_back_clicked.visit_id
    group by 1
)

,clicked_on_bundle as (
    select
        fbq_flow.visit_id
        ,count(event_quiz_nav_purchase_bundle.visit_id) as cnt
    from fbq_flow
        inner join event_quiz_nav_purchase_bundle on fbq_flow.visit_id = event_quiz_nav_purchase_bundle.visit_id
    group by 1
)

,clicked_on_custom as (
    select
        fbq_flow.visit_id
        ,count(event_quiz_nav_custom_bundle.visit_id) as cnt
    from fbq_flow
        inner join event_quiz_nav_custom_bundle on fbq_flow.visit_id = event_quiz_nav_custom_bundle.visit_id
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

,all_events as (
    select
        visit_id
        ,id as event_id
        ,user_id
        ,time as occurred_at_utc
        ,try_parse_json(properties):action::text as event_action
        ,case
            when name = 'custom_event' then try_parse_json(properties):action::text
            else name
        end as event_name
    from raw.cc_cc.ahoy_events
    where convert_timezone('UTC','America/Los_Angeles',time) = :daterange
        and time < dateadd(hour,-1,getdate())
),

last_event as (
    select distinct
            visit_id
            ,last_value(date(occurred_at_utc)) over(partition by visit_id order by occurred_at_utc, event_id) as last_date
            ,last_value(event_name) over(partition by visit_id order by occurred_at_utc, event_id) as last_event
    from all_events
),

abandoned_visits as (
    select *
    from last_event
    where last_event in ('click-build-box','clicked-product-bundle','clicked-customize-product-bundle',
                            'clicked-custom-product-bundle','clicked-back-button','clicked-membership-option')
)
   
select
    fbq_flow.visit_date
    ,count(distinct fbq_flow.visit_id) as fbq_count
    ,count(distinct clicked_nav_back.visit_id)::float/count(distinct fbq_flow.visit_id)::float as "Pct FBQ Backed Out"
    ,count(distinct clicked_on_bundle.visit_id)::float/count(distinct fbq_flow.visit_id)::float as "Pct FBQ Clicked Select Bundle"
    ,count(distinct clicked_on_custom.visit_id)::float/count(distinct fbq_flow.visit_id)::float as "Pct FBQ Clicked on Custom Box"
    ,count(distinct clicked_customize_bundle.visit_id)::float/count(distinct fbq_flow.visit_id)::float as "Pct FBQ Clicked Customize Bundle"
    ,count(distinct abandoned_visits.visit_id)::float/count(distinct fbq_flow.visit_id)::float as "Pct FBQ Abandond Session"
    -- ,(count(distinct clicked_on_bundle.visit_id) + count(distinct clicked_on_custom.visit_id) + count(distinct clicked_customize_bundle.visit_id)::float)
    --         /count(distinct clicked_fbq_cta.visit_id)::float as "Pct Continue to Page 2"
from fbq_flow
    left join clicked_nav_back on fbq_flow.visit_id = clicked_nav_back.visit_id
    left join clicked_on_bundle on fbq_flow.visit_id = clicked_on_bundle.visit_id
    left join clicked_on_custom on fbq_flow.visit_id = clicked_on_custom.visit_id
    left join clicked_customize_bundle on fbq_flow.visit_id = clicked_customize_bundle.visit_id
    left join abandoned_visits on fbq_flow.visit_id = abandoned_visits.visit_id
where
    fbq_flow.is_guest_user = :is_guest
group by 1
order by 1;
