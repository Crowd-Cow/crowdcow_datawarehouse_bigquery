{{
  config(
        snowflake_warehouse = 'TRANSFORMING_M'
    )
}}

with

visits as ( select * from {{ ref('visit_classification') }} )
,suspicious_ips as ( select * from {{ ref('stg_cc__suspicious_ips') }} )
,user_orders as ( select * from {{ ref('int_user_order_activity') }} )
,user_membership as ( select * from {{ ref('int_user_memberships') }} )
,events as ( select * from {{ ref('stg_cc__events') }} )
,ip_detail as ( select * from {{ ref('stg_reference__ip_lookup') }} )

,visit_activity as (
    select 
        visit_id
        ,count(distinct case when event_name = 'SUBSCRIBED' then subscription_id end) as subscribes
        ,count(distinct case when event_name = 'UNSUBSCRIBED' then subscription_id end) as unsubscribes
        ,count_if(event_name = 'SIGN_UP') as sign_ups
        ,count_if(event_name = 'ORDER_COMPLETE') as order_completes
        ,count_if(category = 'PRODUCT' and action = 'VIEW-IMPRESSION') as pcp_impressions
        ,count_if(category = 'PRODUCT' and action = 'IMPRESSION-CLICK') as pcp_impression_clicks
        ,count_if(category = 'PRODUCT' and action = 'PAGE-INTERACTION' and label = 'CLICKED-ADD-TO-CART') as pdp_add_to_carts
        ,count_if(event_name = 'VIEWED_PRODUCT') as viewed_pdps
        ,count_if(event_sequence_number = 1 and event_name = 'PAGE_VIEW' and parse_url(url):path::text in ('','L')) as homepage_views
        ,count_if(event_name = 'CLICK' and label in ('GET STARTED','CLAIM OFFER') and on_page_path like '%/LANDING%') as landing_offer_claim
        ,count_if(event_name = 'CLICK' and label = 'SKIP' and on_page_path like '%/LANDING%') as landing_offer_skiped
        ,count( distinct case when event_name = 'EXPERIMENT_ASSIGNED_TO_SESSION' and experiments:"exp-cc-home_page_redirect"::STRING = 'experimental' then visit_id end) as home_page_redirect_experimental
        ,count( distinct case when event_name = 'EXPERIMENT_ASSIGNED_TO_SESSION' and experiments:"exp-cc-home_page_redirect"::STRING = 'control' then visit_id end) as home_page_redirect_control
        ,count(*) as event_count
    from events
    group by 1
)

,visit_clean_urls as (
    select
        visit_id
        ,user_id
        ,visit_referrer
        ,visit_landing_page_path
        ,visit_landing_page_host = 'WWW.CROWDCOW.COM' 
            and visit_landing_page_path in ('/','/L') as is_homepage_landing

        ,visit_ip
        ,utm_content
        ,visit_user_agent
        ,started_at_utc
    from visits
)

,add_flags as (
    select
        visit_clean_urls.visit_id
        ,visit_clean_urls.user_id
        ,visit_clean_urls.visit_referrer
        ,visit_clean_urls.is_homepage_landing
        ,suspicious_ips.visit_ip is not null
            or visit_clean_urls.visit_user_agent like any ('%BOT%','%CRAWL%','%LIBRATO%','%TWILIOPROXY%','%YAHOOMAILPROXY%','%SCOUTURLMONITOR%','%FULLCONTACT%','%IMGIX%','%BUCK%')
            or (visit_clean_urls.visit_ip is null and visit_clean_urls.visit_user_agent is null) as is_bot
        ,visit_clean_urls.visit_ip in ('66.171.181.219', '127.0.0.1') or (user_orders.user_id is not null and user_orders.user_type in ('EMPLOYEE','INTERNAL')) as is_internal_traffic
        ,iff(ip_detail.is_server and user_orders.first_completed_order_date is null,TRUE,FALSE) as is_server        
        ,user_orders.user_id is not null and user_orders.customer_cohort_date < visit_clean_urls.started_at_utc as has_previous_order
        ,user_orders.user_id is not null and user_orders.first_completed_order_date < visit_clean_urls.started_at_utc as has_previous_completed_order
        ,user_membership.user_id is not null and user_membership.first_membership_created_date < visit_clean_urls.started_at_utc as has_previous_subscription
        ,user_orders.user_id is not null and user_orders.created_at_utc < visit_clean_urls.started_at_utc as had_account_created
        /*,case 
            when (user_orders.last_paid_order_date < dateadd('day',-365,sysdate()) = true or last_paid_order_date is null)
                and user_membership.total_uncancelled_memberships > 0 
                and visit_clean_urls.visit_landing_page_path like any ('%JAPANESE%', 'WAGYU', 'TURKEY', 'GIFT')
                or visits_clean_urls.utm_content = 'ALCNONSUB'
                and user_membership.total_uncancelled_memberships > 0 then 'ALC PROSPECT'
            when not ((user_orders.last_paid_order_date < dateadd('day',-365,sysdate()) = true or last_paid_order_date is null)
                and user_membership.total_uncancelled_memberships > 0 
                and visit_clean_urls.visit_landing_page_path like any ('%JAPANESE%', 'WAGYU', 'TURKEY', 'GIFT')
                or visits_clean_urls.utm_content = 'ALCNONSUB'
                and user_membership.total_uncancelled_memberships > 0)
                and (user_orders.last_paid_order_date < dateadd('day',-365,sysdate()) = true or last_paid_order_date is null)
                and user_membership.total_uncancelled_memberships > 0 then 'SUBSCRIBER PROSPECT'
            when */
            
        ,visit_activity.visit_id is not null and subscribes - unsubscribes > 0 as did_subscribe
        ,visit_activity.visit_id is not null and subscribes - unsubscribes < 0 as did_unsubscribe
        ,visit_activity.visit_id is not null and sign_ups > 0 as did_sign_up
        ,visit_activity.visit_id is not null and order_completes > 0 as did_complete_order
        ,case 
            when visit_activity.home_page_redirect_experimental > 0  then 'EXPERIMENTAL'   
            when visit_activity.home_page_redirect_control > 0 then 'CONTROL'
        else null end as home_page_redirect 
        ,visit_clean_urls.is_homepage_landing and (visit_activity.visit_id is null or (visit_activity.homepage_views = 1 and visit_activity.event_count = 1)) as did_bounce_homepage
        ,zeroifnull(visit_activity.pcp_impressions) as pcp_impressions_count
        ,zeroifnull(visit_activity.pcp_impression_clicks) as pcp_impression_clicks_count
        ,zeroifnull(visit_activity.pdp_add_to_carts) as pdp_product_add_to_cart_count
        ,zeroifnull(visit_activity.viewed_pdps) as pdp_views_count
        ,case 
            when landing_offer_claim = 0 and landing_offer_skiped = 0 then null
            when landing_offer_claim > landing_offer_skiped then 'CLAIM' 
            when landing_offer_claim > 0 and landing_offer_skiped > 0 then 'CLAIM'
            when landing_offer_claim = 0 and landing_offer_skiped > 0 then 'SKIPPED'
            else null end as landing_offer
    from visit_clean_urls
        left join suspicious_ips on visit_clean_urls.visit_ip = suspicious_ips.visit_ip
        left join user_orders on visit_clean_urls.user_id = user_orders.user_id
        left join user_membership on visit_clean_urls.user_id = user_membership.user_id
        left join visit_activity on visit_clean_urls.visit_id = visit_activity.visit_id
        left join ip_detail on visit_clean_urls.visit_ip = ip_detail.ip_address
)

,define_prospects as (
    select
        *
        ,(not has_previous_completed_order or has_previous_completed_order is null)
            and not is_bot
            and not is_internal_traffic
            and not is_server
            and (not has_previous_subscription or user_id is null)
            and (not(visit_referrer like any ('%ZENDESK%','%ADMIN%','%TRACKING-INFO','%SHIPMENT-IN-TRANSIT')) 
                    or visit_referrer is null) as is_prospect
    from add_flags
)

select * from define_prospects
