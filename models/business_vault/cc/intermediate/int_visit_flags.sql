{{
  config(
        cluster_by = ["visit_id","user_id"]
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
        visit_id,
        COUNT(DISTINCT IF(event_name = 'SUBSCRIBED', subscription_id, NULL)) AS subscribes,
        COUNT(DISTINCT IF(event_name = 'UNSUBSCRIBED', subscription_id, NULL)) AS unsubscribes,
        COUNTIF(event_name = 'SIGN_UP') AS sign_ups,
        COUNTIF(event_name = 'ORDER_COMPLETE') AS order_completes,
        COUNTIF(category = 'PRODUCT' AND action = 'VIEW-IMPRESSION') AS pcp_impressions,
        COUNTIF(category = 'PRODUCT' AND action = 'IMPRESSION-CLICK') AS pcp_impression_clicks,
        COUNTIF(category = 'PRODUCT' AND action = 'PAGE-INTERACTION' AND label = 'CLICKED-ADD-TO-CART') AS pdp_add_to_carts,
        COUNTIF(event_name = 'VIEWED_PRODUCT') AS viewed_pdps,
        COUNTIF(event_sequence_number = 1 AND event_name = 'PAGE_VIEW' AND REGEXP_CONTAINS(url, r'^$|^L$')) AS homepage_views,
        COUNTIF(event_name = 'CLICK' AND label IN ('GET STARTED', 'CLAIM OFFER') AND on_page_path LIKE '%/LANDING%') AS landing_offer_claim,
        COUNTIF(event_name = 'CLICK' AND label = 'SKIP' AND on_page_path LIKE '%/LANDING%') AS landing_offer_skipped,
        COUNT(DISTINCT IF(event_name = 'EXPERIMENT_ASSIGNED_TO_SESSION' AND JSON_EXTRACT_SCALAR(experiments, '$.exp-cc-home_page_redirect') = 'experimental', visit_id, NULL)) AS home_page_redirect_experimental,
        COUNT(DISTINCT IF(event_name = 'EXPERIMENT_ASSIGNED_TO_SESSION' AND JSON_EXTRACT_SCALAR(experiments, '$.exp-cc-home_page_redirect') = 'control', visit_id, NULL)) AS home_page_redirect_control,
        COUNT(DISTINCT CASE WHEN event_name = 'EXPERIMENT_ASSIGNED_TO_SESSION' AND JSON_EXTRACT_SCALAR(experiments, '$.exp-cc-hp_redirect_2') = 'experimental'  THEN visit_id END) AS hp_redirect_2_experimental,
        COUNT(DISTINCT CASE WHEN event_name = 'EXPERIMENT_ASSIGNED_TO_SESSION' AND JSON_EXTRACT_SCALAR(experiments, '$.exp-cc-hp_redirect_2') = 'control'  THEN visit_id END) AS hp_redirect_2_control, 
        COUNT(DISTINCT CASE WHEN event_name = 'EXPERIMENT_ASSIGNED_TO_SESSION' AND JSON_EXTRACT_SCALAR(experiments, '$.exp-cc-express_checkout') = 'control'  THEN visit_id END) AS express_checkout_control,
        COUNT(DISTINCT CASE WHEN event_name = 'EXPERIMENT_ASSIGNED_TO_SESSION' AND JSON_EXTRACT_SCALAR(experiments, '$.exp-cc-express_checkout') = 'experimental'  THEN visit_id END) AS express_checkout_experimental, 
        COUNT(*) AS event_count
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
        ,utm_campaign
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
        ,if(ip_detail.is_server and user_orders.first_completed_order_date is null,TRUE,FALSE) as is_server        
        ,user_orders.user_id is not null and user_orders.customer_cohort_date < cast(visit_clean_urls.started_at_utc as date)  as has_previous_order
        ,user_orders.user_id is not null and user_orders.first_completed_order_date < cast(visit_clean_urls.started_at_utc as timestamp) as has_previous_completed_order
        ,user_membership.user_id is not null and user_membership.first_membership_created_date < cast(visit_clean_urls.started_at_utc as timestamp) as has_previous_subscription
        ,user_orders.user_id is not null and user_orders.created_at_utc < cast(visit_clean_urls.started_at_utc as timestamp) as had_account_created
        ,user_orders.last_paid_order_date < DATE_SUB(current_date(), INTERVAL 365 DAY) OR user_orders.last_paid_order_date IS NULL or (user_orders.user_id is not null and user_orders.first_completed_order_date < cast(visit_clean_urls.started_at_utc as timestamp)) is null  as  no_orders_12_months
        ,CASE
            WHEN 
                (user_orders.last_paid_order_date < DATE_SUB(current_date(), INTERVAL 365 DAY) OR user_orders.last_paid_order_date IS NULL or visit_clean_urls.user_id is null)
                and user_membership.total_uncancelled_memberships <= 0 or user_membership.total_uncancelled_memberships is null 
                AND (
                    REGEXP_CONTAINS(visit_clean_urls.visit_landing_page_path,'JAPANESE') 
                    OR REGEXP_CONTAINS(visit_clean_urls.visit_landing_page_path,'WAGYU')
                    OR REGEXP_CONTAINS(visit_clean_urls.visit_landing_page_path,'TURKEY')
                    OR REGEXP_CONTAINS(visit_clean_urls.visit_landing_page_path,'GIFT')
                    OR visit_clean_urls.utm_content = 'ALCNONSUB'
                )
            THEN 'ALC PROSPECT'
            WHEN 
                 user_membership.total_uncancelled_memberships > 0
            THEN 'ACTIVE SUBSCRIBER'
            
            WHEN 
                (user_orders.last_paid_order_date < DATE_SUB(current_date(), INTERVAL 365 DAY) OR user_orders.last_paid_order_date IS NULL  or visit_clean_urls.user_id is null)
                AND user_membership.total_uncancelled_memberships <= 0 or user_membership.total_uncancelled_memberships is null 
            THEN 'SUBSCRIBER PROSPECT'
            
            ELSE 'DEFAULT'
        END AS segment_definitions
        ,visit_activity.visit_id is not null and subscribes - unsubscribes > 0 as did_subscribe
        ,visit_activity.visit_id is not null and subscribes - unsubscribes < 0 as did_unsubscribe
        ,visit_activity.visit_id is not null and sign_ups > 0 as did_sign_up
        ,visit_activity.visit_id is not null and order_completes > 0 as did_complete_order
        ,case 
            when visit_activity.home_page_redirect_experimental > 0  then 'EXPERIMENTAL1.0'   
            when visit_activity.home_page_redirect_control > 0 then 'CONTROL1.0'
            when visit_activity.hp_redirect_2_experimental > 0 then 'EXPERIMENTAL2.0'   
            when visit_activity.hp_redirect_2_control > 0 then 'CONTROL2.0'   
        else null end as home_page_redirect 
        ,case 
            when visit_activity.express_checkout_experimental > 0  then 'EXPERIMENTAL1.0'   
            when visit_activity.express_checkout_control > 0 then 'CONTROL1.0' 
        else null end as express_checkout 
        ,visit_clean_urls.is_homepage_landing and (visit_activity.visit_id is null or (visit_activity.homepage_views = 1 and visit_activity.event_count = 1)) as did_bounce_homepage
        ,coalesce(visit_activity.pcp_impressions) as pcp_impressions_count
        ,coalesce(visit_activity.pcp_impression_clicks) as pcp_impression_clicks_count
        ,coalesce(visit_activity.pdp_add_to_carts) as pdp_product_add_to_cart_count
        ,coalesce(visit_activity.viewed_pdps) as pdp_views_count
        ,case 
            when landing_offer_claim = 0 and landing_offer_skipped = 0 then null
            when landing_offer_claim > landing_offer_skipped then 'CLAIM' 
            when landing_offer_claim > 0 and landing_offer_skipped > 0 then 'CLAIM'
            when landing_offer_claim = 0 and landing_offer_skipped > 0 then 'SKIPPED'
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
        ,--(not has_previous_completed_order or has_previous_completed_order is null)
             not is_bot
            and not is_internal_traffic
            and not is_server
            and (not has_previous_subscription or user_id is null)
            and (not(visit_referrer like any ('%ZENDESK%','%ADMIN%','%TRACKING-INFO','%SHIPMENT-IN-TRANSIT')) 
            and no_orders_12_months
                    or visit_referrer is null) as is_prospect_12_months
    from add_flags
)

select * from define_prospects
