{{
  config(
        snowflake_warehouse = 'TRANSFORMING_M'
    )
}}

with

visits as ( select * from {{ ref('visits') }} )
,all_events as ( select * from {{ ref('visit_events') }} )
,visit_flags as ( select * from {{ ref('visit_flags') }} )

,find_user_id as (

    select 
        *
        ,last_value(user_id) over(partition by visit_id order by occurred_at_utc, event_id) as last_user_id
    from all_events
)

,aggregate_events as (

    select
        visit_id
        ,last_user_id as user_id
        ,array_agg(event_name) within group (order by occurred_at_utc, event_id)::variant as visit_event_sequence
    from find_user_id
    group by 1,2

)

,joined_visits as (

    select 
        visits.visit_id
        ,coalesce(visits.user_id,aggregate_events.user_id) as user_id
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
        ,visits.channel
        ,visits.sub_channel
        ,visits.visit_attributed_source
        ,visits.is_wall_displayed
        ,visit_flags.is_bot
        ,visit_flags.is_internal_traffic
        ,visit_flags.is_homepage_landing
        ,visit_flags.has_previous_order
        ,visit_flags.has_previous_subscription
        ,visit_flags.had_account_created
        ,visit_flags.did_subscribe
        ,visit_flags.did_sign_up
        ,visit_flags.did_complete_order
        ,visit_flags.pdp_views_count
        ,visit_flags.pcp_impressions_count
        ,visit_flags.pcp_impression_clicks_count
        ,visit_flags.pdp_product_add_to_cart_count
        ,visits.started_at_utc
        ,visits.updated_at_utc
        ,aggregate_events.visit_event_sequence
    from visits
        left join visit_flags on visits.visit_id = visit_flags.visit_id
        left join aggregate_events on visits.visit_id = aggregate_events.visit_id
        

)

select * from joined_visits
