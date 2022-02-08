with 

source as ( select * from {{ source('google_ads', 'campaign_stats') }} )

,renamed as (

    select
        customer_id
        ,date as campaign_stat_date
        ,{{ clean_strings('base_campaign') }} as base_campaign
        ,conversions_value
        ,conversions
        ,interactions
        ,{{ clean_strings('ad_network_type') }} as ad_network_type
        ,interaction_event_types
        ,id as campaign_id
        ,impressions
        ,active_view_viewability
        ,{{ clean_strings('device') }} as device
        ,view_through_conversions
        ,active_view_impressions
        ,clicks
        ,active_view_measurable_impressions
        ,active_view_measurable_cost_micros
        ,active_view_measurability
        ,cost_micros/1000000 as cost

    from source

)

select * from renamed
