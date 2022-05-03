{{
  config(
        snowflake_warehouse = 'TRANSFORMING_M'
    )
}}

with

visits as ( select * from {{ ref('visit_classification') }} )
,visit_flags as ( select * from {{ ref('int_visit_flags') }} )

,get_ip_session as (
    select
        *
        
        /** Assign a sequential session number to the same IP address if the visits are within 30 minutes of each other **/
        /** For example: the first visit for IP address 127.0.0.1 gets a session number of 0. If the second visit for the same IP address is within 30 minutes, the session number stays 0. **/
        /** If the next visit for the same IP address is more than 30 minutes from the previous visit, the session number increments to 1 **/
        ,conditional_true_event(datediff(hour, lag(started_at_utc) over (partition by visit_ip order by started_at_utc), started_at_utc) >= 24) over(partition by visit_ip order by started_at_utc) as ip_session_number

    from visits
)

,joined_visits as (

    select 
        get_ip_session.visit_id
        ,get_ip_session.user_id
        ,get_ip_session.partner_id
        ,get_ip_session.visit_token
        ,get_ip_session.visitor_token
        ,get_ip_session.visit_browser
        ,get_ip_session.visit_city
        ,get_ip_session.visit_region
        ,get_ip_session.visit_country
        ,get_ip_session.visit_ip

        /** Combine the IP address with the sequential session number to create a unique session ID for that IP address **/
        /** Note: This session ID is not unique per day. For example: On day one, at 11:59 pm, IP address is assigned a session ID of 127.0.0.1-0 **/
        /** On day two, at 1:45 am more than 30 minutes from the previous visit, the session ID is now 127.0.0.1-1 and does not start over at 127.0.0.1-0 **/
        ,get_ip_session.visit_ip || '-' || get_ip_session.ip_session_number as visitor_ip_session
        
        ,get_ip_session.visit_os
        ,get_ip_session.visit_device_type
        ,get_ip_session.visit_user_agent
        ,get_ip_session.visit_referrer
        ,get_ip_session.visit_referring_domain
        ,get_ip_session.visit_search_keyword
        ,get_ip_session.visit_landing_page
        ,get_ip_session.visit_landing_page_path
        ,get_ip_session.utm_content
        ,get_ip_session.utm_campaign
        ,get_ip_session.utm_adset
        ,get_ip_session.utm_term
        ,get_ip_session.utm_medium
        ,get_ip_session.utm_source
        ,get_ip_session.channel
        ,get_ip_session.sub_channel
        ,get_ip_session.visit_attributed_source
        ,get_ip_session.is_wall_displayed
        ,get_ip_session.is_paid_referrer
        ,get_ip_session.is_social_platform_referrer
        ,visit_flags.is_bot
        ,visit_flags.is_internal_traffic
        ,visit_flags.is_invalid_visit
        ,visit_flags.is_homepage_landing
        ,visit_flags.has_previous_order
        ,visit_flags.has_previous_completed_order
        ,visit_flags.has_previous_subscription
        ,visit_flags.had_account_created
        ,visit_flags.did_subscribe
        ,visit_flags.did_unsubscribe
        ,visit_flags.did_sign_up
        ,visit_flags.did_complete_order
        ,visit_flags.did_bounce_homepage
        ,visit_flags.pdp_views_count
        ,visit_flags.pcp_impressions_count
        ,visit_flags.pcp_impression_clicks_count
        ,visit_flags.pdp_product_add_to_cart_count
        ,get_ip_session.started_at_utc
        ,get_ip_session.updated_at_utc
    from get_ip_session
        left join visit_flags on get_ip_session.visit_id = visit_flags.visit_id
    where not visit_flags.is_invalid_visit
        and get_ip_session.visit_landing_page <> ''
        

)

select 
    * 
    /** Adds the row number per visitor_ip_session so that marketing can isolate the first visit for this IP session in order to attribute the source of the visit appropriately **/
    ,row_number() over(partition by visitor_ip_session order by started_at_utc, visit_id) as ip_session_visit_number
from joined_visits
