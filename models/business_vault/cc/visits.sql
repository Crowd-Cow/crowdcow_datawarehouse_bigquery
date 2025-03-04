{{
  config(
        partition_by = {"field": "started_at_utc", "data_type": "timestamp" },
        cluster_by = ["visit_id","user_id"]
    )
}}

with

visits as ( select * from {{ ref('visit_classification') }} )
,visit_flags as ( select * from {{ ref('int_visit_flags') }} )

,SessionStartFlags AS (
  SELECT
    *,
    CASE
      WHEN TIMESTAMP_DIFF(started_at_utc, LAG(started_at_utc) OVER (PARTITION BY visit_ip ORDER BY started_at_utc), MINUTE) >= 420 OR LAG(started_at_utc) OVER (PARTITION BY visit_ip ORDER BY started_at_utc) IS NULL THEN 1
      ELSE 0
    END AS new_session_start_flag
  FROM
    visits
)

, get_ip_session AS (
  SELECT
    *,
    /** Assign a sequential session number to the same IP address if the visits are within 30 minutes of each other **/
        /** For example: the first visit for IP address 127.0.0.1 gets a session number of 0. If the second visit for the same IP address is within 30 minutes, the session number stays 0. **/
        /** If the next visit for the same IP address is more than 30 minutes from the previous visit, the session number increments to 1 **/
    SUM(new_session_start_flag) OVER (PARTITION BY visit_ip ORDER BY started_at_utc) AS ip_session_number
  FROM
    SessionStartFlags
)

,joined_visits as (

    select 
        get_ip_session.visit_id
        ,get_ip_session.user_id
        ,get_ip_session.partner_id
        ,get_ip_session.visit_token
        ,get_ip_session.visitor_token
        ,get_ip_session.visit_browser
        --,get_ip_session.visit_city
        --,get_ip_session.visit_region
        --,get_ip_session.visit_country
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
        --,get_ip_session.visit_search_keyword
        ,get_ip_session.visit_landing_page
        ,get_ip_session.visit_landing_page_path
        ,get_ip_session.utm_content
        ,get_ip_session.utm_campaign
        ,get_ip_session.utm_adset
        ,get_ip_session.utm_term
        ,get_ip_session.utm_medium
        ,get_ip_session.utm_source
        ,get_ip_session.gclid
        ,get_ip_session.u_token
        ,get_ip_session.channel
        ,get_ip_session.sub_channel
        ,get_ip_session.visit_attributed_source
        ,get_ip_session.is_wall_displayed
        ,get_ip_session.is_paid_referrer
        ,get_ip_session.is_social_platform_referrer
        ,visit_flags.is_bot
        ,visit_flags.is_internal_traffic
        ,visit_flags.is_server
        ,visit_flags.is_homepage_landing
        ,visit_flags.is_prospect
        ,visit_flags.has_previous_order
        ,visit_flags.has_previous_completed_order
        ,visit_flags.has_previous_subscription
        ,visit_flags.had_account_created
        ,visit_flags.did_subscribe
        ,visit_flags.did_unsubscribe
        ,visit_flags.express_checkout
        ,visit_flags.tocc_redirect
        ,visit_flags.did_sign_up
        ,visit_flags.did_complete_order
        ,visit_flags.did_bounce_homepage
        ,visit_flags.pdp_views_count
        ,visit_flags.pcp_impressions_count
        ,visit_flags.pcp_impression_clicks_count
        ,visit_flags.pdp_product_add_to_cart_count
        ,visit_flags.landing_offer
        ,visit_flags.home_page_redirect
        ,visit_flags.is_prospect_12_months
        ,visit_flags.segment_definitions
        ,visit_flags.session_duration
        ,visit_flags.engaged_session
        ,get_ip_session.started_at_utc
        ,get_ip_session.updated_at_utc
    from get_ip_session
        left join visit_flags on get_ip_session.visit_id = visit_flags.visit_id
        

)

select 
    * 
    /** Adds the row number per visitor_ip_session so that marketing can isolate the first visit for this IP session in order to attribute the source of the visit appropriately **/
    ,row_number() over(partition by visitor_ip_session order by started_at_utc, visit_id) as ip_session_visit_number
from joined_visits
