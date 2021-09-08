/*** Snowflake prototype dashboard: https://app.snowflake.com/us-east-1/lna65058/first-box-dashboard-includes-any-visitor-that-saw-the-homepage-with-fbq-dZ6KnqQC4 ****/
/**** FBQ CTA and Sign Up Visit Volume - Visit had both events. Sign up was not necessarily reached through FBQ ****/

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
)

,clicked_nav_back as (
    select
        clicked_fbq_cta.visit_id
        ,count(event_quiz_nav_back_clicked.visit_id) as cnt
    from clicked_fbq_cta
        inner join event_quiz_nav_back_clicked on clicked_fbq_cta.visit_id = event_quiz_nav_back_clicked.visit_id
    group by 1
)

,clicked_on_bundle as (
    select
        clicked_fbq_cta.visit_id
        ,count(event_quiz_nav_purchase_bundle.visit_id) as cnt
    from clicked_fbq_cta
        inner join event_quiz_nav_purchase_bundle on clicked_fbq_cta.visit_id = event_quiz_nav_purchase_bundle.visit_id
    group by 1
)

,clicked_on_custom as (
    select
        clicked_fbq_cta.visit_id
        ,count(event_quiz_nav_custom_bundle.visit_id) as cnt
    from clicked_fbq_cta
        inner join event_quiz_nav_custom_bundle on clicked_fbq_cta.visit_id = event_quiz_nav_custom_bundle.visit_id
    group by 1
)

,clicked_customize_bundle as (
    select
        clicked_fbq_cta.visit_id
        ,count(event_quiz_nav_customize_bundle.visit_id) as cnt
    from clicked_fbq_cta
        inner join event_quiz_nav_customize_bundle on clicked_fbq_cta.visit_id = event_quiz_nav_customize_bundle.visit_id
    group by 1
)

,sign_in_event_fbq as (
    select clicked_fbq_cta.visit_id
  ,count(raw.cc_cc.ahoy_events.visit_id) as sign_in_count
from clicked_fbq_cta
    inner join raw.cc_cc.ahoy_events on raw.cc_cc.ahoy_events.visit_id = clicked_fbq_cta.visit_id
where convert_timezone('UTC','America/Los_Angeles',raw.cc_cc.ahoy_events.time) = :daterange
  and raw.cc_cc.ahoy_events.name = 'sign_up'
group by 1
)

,sign_in_from_customized_bundle as (
    select clicked_customize_bundle.visit_id
    ,count(distinct raw.cc_cc.ahoy_events.visit_id) as select_bundle_sign_in
    from clicked_customize_bundle
    inner join raw.cc_cc.ahoy_events on raw.cc_cc.ahoy_events.visit_id = clicked_customize_bundle.visit_id
where convert_timezone('UTC','America/Los_Angeles',raw.cc_cc.ahoy_events.time) = :daterange
  and raw.cc_cc.ahoy_events.name = 'sign_up'
group by 1
)

,sign_in_from_bundle_select as (
    select clicked_on_bundle.visit_id
    ,count(distinct raw.cc_cc.ahoy_events.visit_id) as select_bundle_sign_in
    from clicked_on_bundle
    inner join raw.cc_cc.ahoy_events on raw.cc_cc.ahoy_events.visit_id = clicked_on_bundle.visit_id
where convert_timezone('UTC','America/Los_Angeles',raw.cc_cc.ahoy_events.time) = :daterange
  and raw.cc_cc.ahoy_events.name = 'sign_up'
group by 1
)

,sign_in_from_custom_bundle as (
    select clicked_on_custom.visit_id
    ,count(distinct raw.cc_cc.ahoy_events.visit_id) as select_bundle_sign_in
    from clicked_on_custom
    inner join raw.cc_cc.ahoy_events on raw.cc_cc.ahoy_events.visit_id = clicked_on_custom.visit_id
where convert_timezone('UTC','America/Los_Angeles',raw.cc_cc.ahoy_events.time) = :daterange
  and raw.cc_cc.ahoy_events.name = 'sign_up'
group by 1
)

select
    clicked_fbq_cta.visit_date
    ,count(distinct sign_in_event_fbq.visit_id) as "Entered FBQ and Sign Up"
    ,count(distinct sign_in_from_bundle_select.visit_id) as "Visits with Sign Up and Selected Bundle"
    ,count(distinct sign_in_from_customized_bundle.visit_id ) as "Visits with Sign Up and Selected Customized Bundle"
    ,count(distinct sign_in_from_custom_bundle.visit_id) as "Visits with Sign Up and Selected Custom Bundle"
from clicked_fbq_cta
    left join sign_in_event_fbq on clicked_fbq_cta.visit_id = sign_in_event_fbq.visit_id
    left join sign_in_from_bundle_select on clicked_fbq_cta.visit_id = sign_in_from_bundle_select.visit_id
    left join sign_in_from_customized_bundle on clicked_fbq_cta.visit_id = sign_in_from_customized_bundle.visit_id
    left join sign_in_from_custom_bundle on clicked_fbq_cta.visit_id = sign_in_from_custom_bundle.visit_id
where
    click_fbq_cta.is_guest_user = :is_guest
group by 1
order by 1;
