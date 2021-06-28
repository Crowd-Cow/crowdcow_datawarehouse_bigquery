with

all_events as ( select * from {{ ref('int_visit_events__unioned') }} ),
visits as ( select * from {{ ref('stg_cc__event_visits') }} ),
subscribed as ( select * from {{ ref('stg_cc__event_subscribed') }} ),
unsubscribed as ( select * from {{ ref('stg_cc__event_unsubscribed') }} ),

aggregate_events as (

    select
        visit_id
        ,array_agg(event_name) within group (order by occurred_at_utc, event_id)::variant as visit_event_sequence
    from all_events
    group by 1

),

subscription_visits as (
    select 
         subscribed.visit_id
        ,count(distinct subscribed.subscription_id) as subscribe_count
        ,count(distinct unsubscribed.subscription_id) as unsubscribe_count
    from subscribed
        left join unsubscribed on (subscribed.visit_id = unsubscribed.visit_id and subscribed.subscription_id = unsubscribed.subscription_id)
    group by 1
    having count(distinct subscribed.subscription_id) - count(distinct unsubscribed.subscription_id) > 0
),

joined_visits as (

    select 
        visits.visit_id
        ,visits.user_id
        ,visits.visit_token
        ,visits.visitor_token
        ,visits.visit_browser
        ,visits.visit_city
        ,visits.visit_region
        ,visits.visit_ip
        ,visits.visit_os
        ,visits.visit_device_type
        ,visits.visit_user_agent

        ,case
            when /* dsia.ip_address IS NOT NULL TODO: add suspicious IP logic
                or */ visits.user_agent LIKE '%BOT%'
                or lower(visits.user_agent) like '%CRAWL%'
                or lower(visits.user_agent) like '%LIBRATO%'
                or lower(visits.user_agent) like '%TWILIOPROXY%'
                or lower(visits.user_agent) like '%YAHOOMAILPROXY%'
                or lower(visits.user_agent) like '%SCOUTURLMONITOR%'
                or lower(visits.user_agent) like '%FULLCONTACT%'
                or lower(visits.user_agent) like '%IMGIX%'
                or lower(visits.user_agent) like '%BUCK%'
                or (visits.ip is null and visits.user_agent is null) THEN true
            else false
         end as is_bot

        ,visits.visit_referrer
        ,visits.visit_referring_domain
        ,visits.visit_search_keyword
        ,visits.visit_landing_page
        ,visits.visit_landing_page_path
        ,visits.is_homepage_landing
        
        ,case 
            when subscription_visits.visit_id is not null then true
            else false
         end as did_subscribe

        ,visits.utm_content
        ,visits.utm_campaign
        ,visits.utm_term
        ,visits.utm_medium
        ,visits.utm_source
        ,visits.started_at_utc
        ,visits.updated_at_utc
        ,aggregate_events.visit_event_sequence
    from visits
        inner join aggregate_events on visits.visit_id = aggregate_events.visit_id
        left join subscription_visits on visits.visit_id = subscription_visits.visit_id

)

select * from joined_visits