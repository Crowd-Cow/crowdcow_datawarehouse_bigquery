with

campaign_history as ( select * from {{ ref('stg_google_ads__campaign_history') }} )
,campaign_stats as ( select * from {{ ref('stg_google_ads__campaign_stats') }} )

,campaigns as (
    select
        campaign_id
        ,campaign_name
        ,advertising_channel_type
        ,row_number() over(partition by campaign_id order by updated_at_utc desc) as rn
    from campaign_history
    qualify rn = 1
)

,summary_stats as (
    select
        {{ dbt_utils.surrogate_key( ['campaign_stat_date','campaign_name','advertising_channel_type'] ) }} as campaign_performance_id
        ,campaign_stats.campaign_stat_date
        ,campaigns.campaign_name
        ,campaigns.advertising_channel_type
        ,sum(campaign_stats.cost) as spend
        ,sum(campaign_stats.impressions) as impressions
        ,sum(campaign_stats.clicks) as clicks
        ,sum(campaign_stats.view_through_conversions) as view_through_conversions
    from campaign_stats
        left join campaigns on campaign_stats.campaign_id = campaigns.campaign_id
    group by 1,2,3,4

)

select * from summary_stats