{{
  config(
        snowflake_warehouse = 'TRANSFORMING_M'
    )
}}

with

visits as ( select * from {{ ref('visit_classification') }} )
,suspicious_ips as ( select * from {{ ref('stg_cc__suspicious_ips') }} )
,orders as ( select * from {{ ref('stg_cc__orders') }} )
,subscriptions as ( select * from {{ ref('stg_cc__subscriptions') }} )
,users as ( select * from {{ ref('stg_cc__users') }} )
,events as ( select * from {{ ref('stg_cc__events') }} )

,visit_activity as (
    select 
        visit_id
        ,count(distinct case when event_name = 'subscribed' then subscription_id end) as subscribes
        ,count(distinct case when event_name = 'unsubscribed' then subscription_id end) as unsubscribes
        ,count_if(event_name = 'sign_up') as sign_ups
        ,count_if(event_name = 'order_complete') as order_completes
        ,count_if(category = 'product' and action = 'view-impression') as pcp_impressions
        ,count_if(category = 'product' and action = 'impression-click') as pcp_impression_clicks
        ,count_if(category = 'product' and action = 'page_interaction' and label = 'clicked-add-to-cart') as pdp_add_to_carts
        ,count_if(event_name = 'viewed_product') as viewed_pdps
    from events
    group by 1
)

,user_first_order as (
    select
        user_id
        ,min(order_paid_at_utc) as first_order_date
    from orders
    where order_paid_at_utc is not null
    group by 1
)

,user_first_completed_order as (
    select
        user_id
        ,min(order_checkout_completed_at_utc) as first_completed_order_date
    from orders
    where order_checkout_completed_at_utc is not null
    group by 1
)

,user_first_subscription as (
    select 
        user_id
        ,min(subscription_created_at_utc) as first_subscription_date
    from subscriptions
    group by 1
)

,user_account_created as (
    select
        user_id
        ,min(created_at_utc) as first_creation_date
    from users
    group by 1
)

,employee_user as (
    select distinct
        user_id
    from users
    where user_type = 'EMPLOYEE'
)

,add_flags as (
    select
        visits.visit_id

        ,visits.visit_landing_page_host = 'WWW.CROWDCOW.COM' 
            and visits.visit_landing_page_path in ('/','/L') as is_homepage_landing

        ,suspicious_ips.visit_ip is not null
            or visits.visit_user_agent like any ('%BOT%','%CRAWL%','%LIBRATO%','%TWILIOPROXY%','%YAHOOMAILPROXY%','%SCOUTURLMONITOR%','%FULLCONTACT%','%IMGIX%','%BUCK%')
            or (visits.visit_ip is null and visits.visit_user_agent is null) as is_bot

        ,visit_landing_page_path like any ('%.JS%','%.ICO%','%.PNG%','%.CSS%','%.PHP%','%.TXT%','%GRAPHQL%'
                                       ,'%.XML%','%.SQL%','%.ICS%','%WELL-KNOWN%','%/e/%','%.ENV%','%/WP-%','/CROWDCOW.COM%'
                                       ,'%/WWW.CROWDCOW.COM%.%','%/ADMIN%','%/INGREDIENT-LIST%','%.','%PHPINFO%','%.YML%'
                                       ,'%.HTML%','%.ASP','%XXXSS%','%.RAR','%.AXD%','%.AWS%','%;VAR%') as is_invalid_visit
        
        ,visits.visit_ip in ('66.171.181.219', '127.0.0.1') or employee_user.user_id is not null as is_internal_traffic
        ,user_first_order.user_id is not null and user_first_order.first_order_date < visits.started_at_utc as has_previous_order
        ,user_first_completed_order.user_id is not null and user_first_completed_order.first_completed_order_date < visits.started_at_utc as has_previous_completed_order
        ,user_first_subscription.user_id is not null and user_first_subscription.first_subscription_date < visits.started_at_utc as has_previous_subscription
        ,user_account_created.user_id is not null and user_account_created.first_creation_date < visits.started_at_utc as had_account_created
        ,visit_activity.visit_id is not null and subscribes - unsubscribes > 0 as did_subscribe
        ,visit_activity.visit_id is not null and sign_ups > 0 as did_sign_up
        ,visit_activity.visit_id is not null and order_completes > 0 as did_complete_order
        ,zeroifnull(visit_activity.pcp_impressions) as pcp_impressions_count
        ,zeroifnull(visit_activity.pcp_impression_clicks) as pcp_impression_clicks_count
        ,zeroifnull(visit_activity.pdp_add_to_carts) as pdp_product_add_to_cart_count
        ,zeroifnull(visit_activity.viewed_pdps) as pdp_views_count

    from visits
        left join suspicious_ips on visits.visit_ip = suspicious_ips.visit_ip
        left join user_first_order on visits.user_id = user_first_order.user_id
        left join user_first_completed_order on visits.user_id = user_first_completed_order.user_id
        left join user_first_subscription on visits.user_id = user_first_subscription.user_id
        left join user_account_created on visits.user_id = user_account_created.user_id
        left join employee_user on visits.user_id = employee_user.user_id
        left join visit_activity on visits.visit_id = visit_activity.visit_id
)

select * from add_flags
