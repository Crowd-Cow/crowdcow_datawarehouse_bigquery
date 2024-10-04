{{ config(
  enabled=false
) }}
with 

source as ( select * from {{ source('google_ads', 'keyword_stats') }} )

,renamed as (
    select 
        DATE as occurred_at_date
        ,{{ clean_strings('ad_network_type') }} as ad_network_type
        ,campaign_id
        ,ad_group_id
        ,ad_group_criterion_criterion_id
        ,impressions
        ,clicks
        ,cost_micros*power(10,-6) as cost_usd
        ,{{ clean_strings('device') }} as device
        ,active_view_measurable_impressions
        ,active_view_measurable_cost_micros
        ,active_view_measurability
        ,active_view_impressions
        ,conversions_value
        ,conversions
        ,interactions
        ,view_through_conversions
    from source
)

select * from renamed
