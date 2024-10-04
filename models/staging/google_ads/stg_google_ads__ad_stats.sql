{{ config(
  enabled=false
) }}
with 

source as ( select * from {{ source('google_ads', 'ad_stats') }} )

,renamed as (

    select
        ad_id
        ,customer_id
        ,campaign_id
        ,ad_group_id
        ,DATE as date_utc
        ,conversions_value
        ,conversions
        ,interactions
        ,{{ clean_strings('ad_network_type') }} as ad_network_type
        ,{{ clean_strings( 'interaction_event_types' )}} as interaction_event_types
        ,impressions
        ,active_view_viewability
        ,{{ clean_strings('device') }} as device
        ,view_through_conversions
        ,active_view_impressions
        ,clicks
        ,active_view_measurable_impressions
        ,cost_micros*power(10,-6) as cost_usd
    from source
)

select * from renamed

