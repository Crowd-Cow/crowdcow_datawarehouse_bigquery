/*** Snowflake prototype dashboard: https://app.snowflake.com/us-east-1/lna65058/first-box-dashboard-includes-any-visitor-that-saw-the-homepage-with-fbq-dZ6KnqQC4 ****/
/**** % of Visitors Who Saw Sign-up and Checkout Step 1 ****/

/* raw.cc_cc.ahoy_events.name = 'sign_up' --> checkout_step_1 --> checkout_step_2 --> order_complete */

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
    having count(subscription_id) > 0
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
        ,fact_visit.visitor_token
        ,case
            when dim_user.email like 'TEMPORARY%CROWDCOW.COM%' then TRUE
            when dim_user.email is null then FALSE
            else FALSE
         end as is_guest_user
        ,members.user_id is not null as is_member
        ,first_subscription_date
        ,convert_timezone('UTC','America/Los_Angeles',fact_visit.visited_at) as visited_at_pst
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

/*LIMITING SIGN UP EVENTS TO 08/1/21 OR LATER */ 
clicked_fbq_cta as (
    select
        homepage_fbq_impressions.*
    from homepage_fbq_impressions
        inner join event_first_box_hero_selected on homepage_fbq_impressions.visit_id = event_first_box_hero_selected.visit_id
),

sign_up_events as ( 
    select
        visit_id 
        , count(id) as event_count
    from raw.cc_cc.ahoy_events 
    where time >= '2021-08-01'
    and name = 'sign_up' 
    group by 1
    having count(id)>0
),

checkout_one_events as ( 
    select 
        fact_event_checkout.visit_id 
        , count(fact_event_checkout.event_id) as event_count
    from fact_event_checkout
    where fact_event_checkout.occurred_at >= '2021-08-01'
        and fact_event_checkout.label = '1'
    group by 1
    having count(fact_event_checkout.event_id) > 0
 
),

checkout_two_events as ( 
    select 
        fact_event_checkout.visit_id 
        , count(fact_event_checkout.event_id) as event_count
    from fact_event_checkout
    where fact_event_checkout.occurred_at >= '2021-08-01'
        and fact_event_checkout.label = '2'
    group by 1
    having count(fact_event_checkout.event_id) > 0
),

order_complete_events as ( 
    select 
        event_order_complete.visit_id 
        , count(event_order_complete.event_id) as event_count
    from event_order_complete
    where event_order_complete.occurred_at >= '2021-08-01'
    group by 1
    having count(event_order_complete.event_id) > 0
)
, viewed_sign_up as (

    select 
    homepage_fbq_impressions.visit_date

    , 
    case when sign_up_events.visit_id is not null then 'Viewed Sign Up' else 'Did Not View Sign Up' end as viewed_sign_up
    , count(distinct case when checkout_one_events.visit_id is not null 
            then homepage_fbq_impressions.visitor_token else null end) as viewed_checkout_one
    , count(distinct case when checkout_two_events.visit_id is not null 
            then homepage_fbq_impressions.visitor_token else null end) as viewed_checkout_two
    , count(distinct case when checkout_one_events.visit_id is not null 
                and checkout_two_events.visit_id is not null 
                and order_complete_events.visit_id is not null
            then homepage_fbq_impressions.visitor_token else null end) as viewed_one_two_order_complete 
    , count(distinct case when order_complete_events.visit_id is not null
            then homepage_fbq_impressions.visitor_token else null end) as viewed_order_complete 
    from homepage_fbq_impressions
    left join sign_up_events on sign_up_events.visit_id = homepage_fbq_impressions.visit_id 
    left join clicked_fbq_cta on homepage_fbq_impressions.visit_id = clicked_fbq_cta.visit_id
     
    left join checkout_one_events on checkout_one_events.visit_id = homepage_fbq_impressions.visit_id
    left join checkout_two_events on checkout_two_events.visit_id = homepage_fbq_impressions.visit_id
    left join order_complete_events on order_complete_events.visit_id = homepage_fbq_impressions.visit_id
    
    /* Where clause filtering for clicked FBQ and were experimental */ 
    where clicked_fbq_cta.visit_id is not null and homepage_fbq_impressions.experiment_variant = 'experimental'
    group by 1,2
)

select visit_date 
    , sum(case when viewed_sign_up = 'Viewed Sign Up' then viewed_checkout_one else null end)/ sum(viewed_checkout_one)::float as "% Viewed sign-up and checkout_1"
    , sum(case when viewed_sign_up = 'Did Not View Sign Up' then viewed_checkout_one else null end)/ sum(viewed_checkout_one)::float as "% Did not view sign-up, but viewed checkout_1"
    
from viewed_sign_up
where
   is_guest_user = :is_guest
group by 1


order by 1 desc
limit 1000;
