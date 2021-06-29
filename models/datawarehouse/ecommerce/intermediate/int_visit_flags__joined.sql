with

visits as ( select * from {{ ref('base_cc__ahoy_visits') }} ),
suspicious_ips as ( select * from {{ ref('stg_cc__suspicious_ips') }} ),
subscribed as ( select * from {{ ref('stg_cc__event_subscribed') }} ),
unsubscribed as ( select * from {{ ref('stg_cc__event_unsubscribed') }} ),

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

add_flags as (

    select
        visits.visit_id
        ,visits.visit_browser
        ,visits.updated_at_utc
        ,visits.visit_city
        ,visits.utm_content
        ,visits.visit_token
        ,visits.visit_ip
        ,visits.utm_campaign
        ,visits.visit_landing_page
        ,visits.visit_landing_page_path

        ,case
            when visits.visit_landing_page_host = 'WWW.CROWDCOW.COM' 
                and visits.visit_landing_page_path = '' or visits.visit_landing_page_path = 'L' then true
            else false
         end as is_homepage_landing

        ,case 
            when subscription_visits.visit_id is not null then true
            else false
         end as did_subscribe

        ,visits.visit_os
        ,visits.utm_term
        ,visits.utm_medium
        ,visits.started_at_utc
        ,visits.visit_referrer
        ,visits.user_id
        ,visits.visit_country
        ,visits.visit_search_keyword
        ,visits.utm_source
        ,visits.visitor_token
        ,visits.visit_device_type
        ,visits.visit_referring_domain
        ,visits.visit_region
        ,visits.visit_user_agent

        ,case
            when  suspicious_ips.visit_ip is not null
                or visits.visit_user_agent like '%BOT%'
                or lower(visits.visit_user_agent) like '%CRAWL%'
                or lower(visits.visit_user_agent) like '%LIBRATO%'
                or lower(visits.visit_user_agent) like '%TWILIOPROXY%'
                or lower(visits.visit_user_agent) like '%YAHOOMAILPROXY%'
                or lower(visits.visit_user_agent) like '%SCOUTURLMONITOR%'
                or lower(visits.visit_user_agent) like '%FULLCONTACT%'
                or lower(visits.visit_user_agent) like '%IMGIX%'
                or lower(visits.visit_user_agent) like '%BUCK%'
                or (visits.visit_ip is null and visits.visit_user_agent is null) then true
            else false
         end as is_bot

         ,visits.is_wall_displayed
    from visits
        left join suspicious_ips on visits.visit_ip = suspicious_ips.visit_ip
        left join subscription_visits on visits.visit_id = subscription_visits.visit_id
)

select * from add_flags
