{{ config(
    materialized='incremental',
    unique_key='visit_id',
    partition_by={'field': 'started_at_utc', 'data_type': 'timestamp'},
    cluster_by=['visit_id','user_id'],
    on_schema_change = 'sync_all_columns'
) }}

with filtered_ip_sessions as (
    -- Limit to recent data (adjust the interval as needed)
    select *
    from {{ ref('int_visit_ip_sessions') }}
    {% if is_incremental() %}
      where started_at_utc > (select max(started_at_utc) from {{ this }})
    {% else %}
     where started_at_utc >= timestamp(DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
    {% endif %}
),

filtered_visit_flags as ( select * from {{ ref('int_visit_flags') }} ),

filtered_ip_detail as (
    select 
        ip_address,
        city,
        region,
        country,
        postal_code
    from {{ ref('stg_reference__ip_lookup') }}
)

,joined_visits as (
    select 
        filtered_ip_sessions.visit_id,
        filtered_ip_sessions.user_id,
        filtered_ip_sessions.partner_id,
        filtered_ip_sessions.visit_token,
        filtered_ip_sessions.visitor_token,
        filtered_ip_sessions.visit_browser,
        filtered_ip_detail.city as visit_city,
        filtered_ip_detail.region as visit_region,
        filtered_ip_detail.country as visit_country,
        filtered_ip_detail.postal_code as visit_postal_code,
        filtered_ip_sessions.visit_ip,
        filtered_ip_sessions.visit_ip || '-' || filtered_ip_sessions.ip_session_number as visitor_ip_session,
        filtered_ip_sessions.visit_os,
        filtered_ip_sessions.visit_device_type,
        filtered_ip_sessions.visit_user_agent,
        filtered_ip_sessions.visit_referrer,
        filtered_ip_sessions.visit_referring_domain,
        filtered_ip_sessions.visit_landing_page,
        filtered_ip_sessions.visit_landing_page_path,
        filtered_ip_sessions.utm_content,
        filtered_ip_sessions.utm_campaign,
        filtered_ip_sessions.utm_adset,
        filtered_ip_sessions.utm_term,
        filtered_ip_sessions.utm_medium,
        filtered_ip_sessions.utm_source,
        filtered_ip_sessions.gclid,
        filtered_ip_sessions.u_token,
        filtered_ip_sessions.channel,
        filtered_ip_sessions.sub_channel,
        filtered_ip_sessions.visit_attributed_source,
        filtered_ip_sessions.is_wall_displayed,
        filtered_ip_sessions.is_paid_referrer,
        filtered_ip_sessions.is_social_platform_referrer,
        filtered_visit_flags.is_bot,
        filtered_visit_flags.is_internal_traffic,
        filtered_visit_flags.is_server,
        filtered_visit_flags.is_homepage_landing,
        filtered_visit_flags.is_prospect,
        filtered_visit_flags.has_previous_order,
        filtered_visit_flags.has_previous_completed_order,
        filtered_visit_flags.has_previous_subscription,
        filtered_visit_flags.had_account_created,
        filtered_visit_flags.did_subscribe,
        filtered_visit_flags.did_unsubscribe,
        filtered_visit_flags.express_checkout,
        filtered_visit_flags.tocc_redirect,
        filtered_visit_flags.did_sign_up,
        filtered_visit_flags.did_complete_order,
        filtered_visit_flags.did_bounce_homepage,
        filtered_visit_flags.pdp_views_count,
        filtered_visit_flags.pcp_impressions_count,
        filtered_visit_flags.pcp_impression_clicks_count,
        filtered_visit_flags.pdp_product_add_to_cart_count,
        filtered_visit_flags.landing_offer,
        filtered_visit_flags.home_page_redirect,
        filtered_visit_flags.is_prospect_12_months,
        filtered_visit_flags.segment_definitions,
        filtered_visit_flags.session_duration,
        filtered_visit_flags.engaged_session,
        filtered_ip_sessions.started_at_utc,
        filtered_ip_sessions.updated_at_utc
    from filtered_ip_sessions 
        left join filtered_visit_flags on filtered_ip_sessions.visit_id = filtered_visit_flags.visit_id
        left join filtered_ip_detail on filtered_ip_sessions.visit_ip = filtered_ip_detail.ip_address
)

select 
    *,
    row_number() over(partition by visitor_ip_session order by started_at_utc, visit_id) as ip_session_visit_number
from joined_visits