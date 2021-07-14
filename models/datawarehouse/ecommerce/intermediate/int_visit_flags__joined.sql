with

visits as ( select * from {{ ref('base_cc__ahoy_visits') }} ),
suspicious_ips as ( select * from {{ ref('stg_cc__suspicious_ips') }} ),
subscribed as ( select * from {{ ref('stg_cc__event_subscribed') }} ),
sign_up as ( select * from {{ ref('stg_cc__event_sign_up') }} ),
order_complete as ( select * from {{ ref('stg_cc__event_order_complete') }} ),
unsubscribed as ( select * from {{ ref('stg_cc__event_unsubscribed') }} ),
orders as ( select * from {{ ref('stg_cc__orders') }} ),
subscriptions as ( select * from {{ ref('stg_cc__subscriptions') }} ),
users as ( select * from {{ ref('stg_cc__users') }} ),

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

user_first_order as (

    select
        user_id
        ,min(order_paid_at_utc) as first_order_date
    from orders
    where order_paid_at_utc is not null
    group by 1
),

user_first_subscription as (

    select 
        user_id
        ,min(subscription_created_at_utc) as first_subscription_date
    from subscriptions
    group by 1
),

user_account_created as (

    select
        user_id
        ,min(created_at_utc) as first_creation_date
    from users
    group by 1
),

user_signed_up as (

    select
        visit_id
        ,count(distinct user_id) as total_signed_up
    from sign_up
    group by 1
),

order_completed as (

    select
        visit_id
        ,count(distinct order_id) as total_order_count
    from order_complete
    group by 1
),

employoee_user as (
    select distinct
        user_id
    from users
    where user_type = 'EMPLOYEE'
),

add_flags as (

    select
        visits.visit_id
        ,visits.user_id
        ,visits.visit_token
        ,visits.visitor_token
        ,visits.visit_browser
        ,visits.visit_city
        ,visits.visit_region
        ,visits.visit_country
        ,visits.visit_ip
        ,visits.visit_os
        ,visits.visit_device_type
        ,visits.visit_user_agent
        ,visits.visit_referrer
        ,visits.visit_referring_domain
        ,visits.visit_search_keyword
        ,visits.visit_landing_page
        ,visits.visit_landing_page_path
        ,visits.utm_content
        ,visits.utm_campaign
        ,visits.utm_term
        ,visits.utm_medium
        ,visits.utm_source
        ,visits.started_at_utc
        ,visits.updated_at_utc
        ,visits.is_wall_displayed

        ,case
            when visits.visit_landing_page_host = 'WWW.CROWDCOW.COM' 
                and visits.visit_landing_page_path = '' or visits.visit_landing_page_path = 'L' then true
            else false
         end as is_homepage_landing

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

        ,case
            when visits.visit_ip in ('66.171.181.219', '127.0.0.1') or employoee_user.user_id then true
            else false
        end as is_internal_traffic

        ,case
            when user_first_order.user_id is not null and user_first_order.first_order_date < visits.started_at_utc then true
            else false
         end as has_previous_order

        ,case
            when user_first_subscription.user_id is not null and user_first_subscription.first_subscription_date < visits.started_at_utc then true
            else false
         end as has_previous_subscription

        ,case
            when user_account_created.user_id is not null and user_account_created.first_creation_date < visits.started_at_utc then true
            else false
         end as had_account_created

        ,subscription_visits.visit_id is not null as did_subscribe
        ,user_signed_up.visit_id is not null as did_sign_up
        ,order_completed.visit_id is not null as did_complete_order

    from visits
        left join suspicious_ips on visits.visit_ip = suspicious_ips.visit_ip
        left join subscription_visits on visits.visit_id = subscription_visits.visit_id
        left join user_first_order on visits.user_id = user_first_order.user_id
        left join user_first_subscription on visits.user_id = user_first_subscription.user_id
        left join user_account_created on visits.user_id = user_account_created.user_id
        left join user_signed_up on visits.visit_id = user_signed_up.visit_id
        left join order_completed on visits.visit_id = order_completed.visit_id
        left join employoee_user on visits.user_id = employoee_user.user_id
)

select * from add_flags
