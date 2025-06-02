{{
  config(
        cluster_by = ["visit_id","user_id"]
    )
}}

with

visits as ( select * from {{ ref('visit_classification') }} )
,user_orders as ( select * from {{ ref('int_user_order_activity') }} )
,user_membership as ( select * from {{ ref('int_user_memberships') }} )
,visit_activity as ( select * from {{ ref('int_visit_activity') }} )
,users as ( select user_id, active_order_id from {{ ref('stg_cc__users') }} where dbt_valid_to is null )
,ip_detail as ( select * from {{ ref('stg_reference__ip_lookup') }} )
,visit_orders as ( select distinct visit_id from  {{ ref('orders') }} where  order_checkout_completed_at_utc is not null)

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
        ,visit_clean_urls.visit_ip in ('66.171.181.219', '127.0.0.1') or (user_orders.user_id is not null and user_orders.user_type in ('EMPLOYEE','INTERNAL')) as is_internal_traffic
        ,if(ip_detail.is_server and user_orders.first_completed_order_date is null,TRUE,FALSE) as is_server        
        ,if(ip_detail.is_proxy and user_orders.first_completed_order_date is null,TRUE,FALSE) as is_proxy
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
        ,case 
            when visit_activity.tocc_redirect_experimental > 0  then 'EXPERIMENTAL1.0'   
            when visit_activity.tocc_redirect_control > 0 then 'CONTROL1.0' 
        else null end as tocc_redirect
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
        ,event_count
        ,TIMESTAMP_DIFF(max_ocurred_event,min_ocurred_event,second) as session_duration
        ,if(page_views >= 2 or clicks >=2 or scroll_depth_25 >= 1 or (add_to_carts > 0 or begin_checkout > 0 or order_completes > 0),true,false) as engaged_session
        ,if(visit_orders.visit_id is not null, true, false) as is_purchasing_visit

    from visit_clean_urls
        left join user_orders on visit_clean_urls.user_id = user_orders.user_id
        left join user_membership on visit_clean_urls.user_id = user_membership.user_id
        left join visit_activity on visit_clean_urls.visit_id = visit_activity.visit_id
        left join ip_detail on visit_clean_urls.visit_ip = ip_detail.ip_address
        left join visit_orders on visit_clean_urls.visit_id = visit_orders.visit_id
)
,define_bots as (
    select
    add_flags.*
    --,((event_count <= 1 or event_count is null) and (not has_previous_completed_order or has_previous_completed_order is null) and (not has_previous_subscription or user_id is null)  ) as is_bot
    from add_flags
)



,define_prospects as (
    select
        define_bots.*
        ,(not has_previous_completed_order or has_previous_completed_order is null)
            --and not is_bot
            and not is_internal_traffic
            and not is_server
            and not is_proxy
            and (not has_previous_subscription or define_bots.user_id is null)
            and (not(visit_referrer like any ('%ZENDESK%','%ADMIN%','%TRACKING-INFO','%SHIPMENT-IN-TRANSIT')) 
                    or visit_referrer is null)
             as is_prospect
        ,--(not has_previous_completed_order or has_previous_completed_order is null)
            -- not is_bot
            not is_internal_traffic
            and not is_server
            and not is_proxy
            and (not has_previous_subscription or define_bots.user_id is null)
            and (not(visit_referrer like any ('%ZENDESK%','%ADMIN%','%TRACKING-INFO','%SHIPMENT-IN-TRANSIT')) 
            and no_orders_12_months
                    or visit_referrer is null) as is_prospect_12_months
    from define_bots
    
)

select * from define_prospects
