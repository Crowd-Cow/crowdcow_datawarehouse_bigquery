with 
 backup as (select * from raw.google_ads_campaign_performance where campaign_stat_date_utc < '2024-10-27' )
,source as (select * from {{ source('google_ads', 'googleads_campaign') }}  where segments_date >= '2024-10-27' )

,merged as (
    select distinct 
        campaign_id
        ,date(campaign_stat_date_utc) as campaign_stat_date_utc
        ,date(null) as campaign_created_date_utc
        ,campaign_name
        ,total_clicks
        ,total_impressions
        ,total_cost_usd
        ,total_conversions
        ,total_conversion_value
        
    from backup
    union all
    select distinct
        campaign_id
        ,date(segments_date) as campaign_stat_date_utc
        ,date(campaign_start_date) as campaign_created_date_utc
        ,{{ clean_strings('campaign_name') }} as campaign_name
        ,sum(metrics_clicks) as total_clicks
        ,sum(metrics_impressions) as total_impressions
        ,sum(metrics_cost_micros/1000000) as total_cost_usd
        ,sum(metrics_conversions) as total_conversions
        ,sum(metrics_conversions_value) as total_conversion_value
       
    from source
    group by 1, 2, 3, 4

)

select * from merged
    
