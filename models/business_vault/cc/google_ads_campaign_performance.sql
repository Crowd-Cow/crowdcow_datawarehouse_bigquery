with 

campaign_stats as (select * from {{ ref('stg_google_ads__campaign_stats')}})
,campaign_history as (select * from {{ ref('stg_google_ads__campaign_history')}} )

,campaign_clicks_cost as (
    select 
        campaign_id
        ,campaign_stat_date_utc
        ,sum(clicks) as total_clicks
        ,sum(impressions) as total_impressions
        ,sum(cost_usd) as total_cost_usd
        ,sum(conversions) as total_conversions
        ,sum(conversions_value) as total_conversion_value
    from campaign_stats
    group by 1, 2
)

,campaign_info as (
    select 
        campaign_id
        ,campaign_name
        ,campaign_valid_from_date
        ,campaign_valid_to_date
    from campaign_history
)


select distinct 
    campaign_clicks_cost.campaign_id
    ,campaign_clicks_cost.campaign_stat_date_utc
    ,campaign_clicks_cost.total_clicks
    ,campaign_clicks_cost.total_impressions
    ,campaign_clicks_cost.total_cost_usd
    ,campaign_clicks_cost.total_conversions
    ,campaign_clicks_cost.total_conversion_value
    ,campaign_info.campaign_name
from campaign_clicks_cost
    left join campaign_info on campaign_clicks_cost.campaign_id = campaign_info.campaign_id
        and campaign_clicks_cost.campaign_stat_date_utc >= campaign_info.campaign_valid_from_date
        and campaign_clicks_cost.campaign_stat_date_utc < campaign_info.campaign_valid_to_date