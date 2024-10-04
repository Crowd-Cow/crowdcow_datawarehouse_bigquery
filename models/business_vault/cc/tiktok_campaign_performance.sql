{{ config(
  enabled=false
) }}
with 

campaign_stats as (select * from {{ ref('stg_tiktok__campaign_report_daily')}})
,campaign_history as (select * from {{ ref('stg_tiktok__campaign_history')}})

select 
    campaign_stats.campaign_id
    ,campaign_history.campaign_name
    ,campaign_history.campaign_type
    ,campaign_history.campaign_status
    ,campaign_stats.video_views_p_100
    ,campaign_stats.video_views_p_75
    ,campaign_stats.video_views_p_50
    ,campaign_stats.video_views_p_25
    ,campaign_stats.video_play_actions
    ,campaign_stats.video_watched_2_s
    ,campaign_stats.video_watched_6_s
    ,campaign_stats.profile_visits_rate
    ,campaign_stats.follows
    ,campaign_stats.ctr
    ,campaign_stats.conversion
    ,campaign_history.budget_usd
    ,campaign_history.budget_mode
    ,campaign_stats.spend
    ,campaign_stats.profile_visits
    ,campaign_stats.clicks
    ,campaign_stats.cpc
    ,campaign_stats.likes
    ,campaign_stats.shares
    ,campaign_stats.impressions    
    ,campaign_stats.reach
    ,campaign_stats.total_purchase_value
    ,campaign_stats.total_purchase
    ,campaign_stats.conversion_rate
    ,campaign_history.opt_status
    ,campaign_history.objective_type
    ,campaign_history.is_new_structure
    ,campaign_history.split_test_variable
    ,campaign_stats.stat_at_date
    ,campaign_history.create_time
    ,campaign_history.updated_at
    ,campaign_history.campaign_valid_from_date
    ,campaign_history.campaign_valid_to_date
from campaign_stats
    join campaign_history on campaign_stats.campaign_id = campaign_history.campaign_id
                          and campaign_stats.stat_at_date >= campaign_history.campaign_valid_from_date
                          and campaign_stats.stat_at_date < campaign_history.campaign_valid_to_date