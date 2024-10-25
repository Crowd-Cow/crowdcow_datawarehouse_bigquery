with source as (select * from {{ source('google_ads', 'googleads_campaign') }} )


    select 
        campaign_id
        ,{{ clean_strings('campaign_name') }} as campaign_name
        ,date(segments_date) as campaign_stat_date_utc
        ,campaign_start_date as campaign_created_date_utc
        ,sum(metrics_clicks) as total_clicks
        ,sum(metrics_impressions) as total_impressions
        ,sum(metrics_cost_micros/1000000) as total_cost_usd
        ,sum(metrics_conversions) as total_conversions
        ,sum(metrics_conversions_value) as total_conversion_value
    from source
    group by 1, 2, 3, 4
