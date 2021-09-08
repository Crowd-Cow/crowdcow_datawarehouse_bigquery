/*** Snowflake prototype dashboard: https://app.snowflake.com/us-east-1/lna65058/first-box-dashboard-includes-any-visitor-that-saw-the-homepage-with-fbq-dZ6KnqQC4 ****/
/**** FBQ Purchase Bundle Percent vs. Purchase Custom Box Percent ****/

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
),

homepage_view_events as (
    select
        visit_id
        ,count(event_id) as event_count
    from fact_event_pageview
    where (parse_url(url):path::text = '' or parse_url(url):path::text = 'l')
        and url not like '%/?first-box%'
    group by 1
),

homepage_fbq_impressions as (
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
        and fact_visit.visit_id in (select visit_id from homepage_view_events)
        and not fact_visit.is_bot
        and not fact_visit.is_internal_traffic
        and valid_experiments.experiment_variant = 'experimental'
        and (utm_medium <> 'FIELD-MARKETING' or utm_medium is null)
        and (members.user_id is null or members.first_subscription_date >= fact_visit.visited_at)
),

clicked_fbq_cta as (
    select
        homepage_fbq_impressions.*
    from homepage_fbq_impressions
        inner join event_first_box_hero_selected on homepage_fbq_impressions.visit_id = event_first_box_hero_selected.visit_id
),

clicked_purchase_bundle_order_complete as (
    select
        clicked_fbq_cta.visit_id
        ,count(event_quiz_nav_purchase_bundle.visit_id) as purchase_count
        ,count(event_order_complete.visit_id) as order_complete_count
    from clicked_fbq_cta
        inner join event_quiz_nav_purchase_bundle on clicked_fbq_cta.visit_id = event_quiz_nav_purchase_bundle.visit_id
        inner join event_order_complete on clicked_fbq_cta.visit_id = event_order_complete.visit_id
    group by 1
),

chose_custom_box_order_complete as (
    select
        clicked_fbq_cta.visit_id
        ,count(event_quiz_nav_custom_bundle.visit_id) as custom_bundle_count
        ,count(event_order_complete.visit_id) as order_complete_count
    from clicked_fbq_cta
        inner join event_quiz_nav_custom_bundle on clicked_fbq_cta.visit_id = event_quiz_nav_custom_bundle.visit_id
        inner join event_order_complete on clicked_fbq_cta.visit_id = event_order_complete.visit_id
    group by 1
)
   
select
    clicked_fbq_cta.visit_date
    ,count(distinct clicked_fbq_cta.visit_id) as fbq_cta_clicked_count
    ,count(distinct clicked_purchase_bundle_order_complete.visit_id)::float/count(distinct clicked_fbq_cta.visit_id)::float as "Pct FBQ Purchased Bundle"
    ,count(distinct chose_custom_box_order_complete.visit_id)::float/count(distinct clicked_fbq_cta.visit_id)::float as "Pct FBQ Purchased Custom Box"
from clicked_fbq_cta
    left join clicked_purchase_bundle_order_complete on clicked_fbq_cta.visit_id = clicked_purchase_bundle_order_complete.visit_id
    left join chose_custom_box_order_complete on clicked_fbq_cta.visit_id = chose_custom_box_order_complete.visit_id
where click_dbq_cta.is_guest_user = :is_guest
group by 1
order by 1;
