{{ config(
  enabled=false
) }}
with 

adgroup_stats as (select * from {{ ref('stg_tiktok__adgroup_report_daily')}})
,adgroup_history as (select * from {{ ref('stg_tiktok__adgroup_history')}})
,campaign_history as (select * from {{ ref('stg_tiktok__campaign_history')}})

,combined as (
    select
        adgroup_stats.adgroup_id
        ,adgroup_stats.stat_at_date
        ,adgroup_stats.video_views_p_100
        ,adgroup_stats.video_views_p_75
        ,adgroup_stats.video_views_p_50
        ,adgroup_stats.video_views_p_25
        ,adgroup_stats.video_play_actions
        ,adgroup_stats.video_watched_2_s
        ,adgroup_stats.video_watched_6_s
        ,adgroup_stats.profile_visits_rate
        ,adgroup_stats.follows
        ,adgroup_stats.ctr
        ,adgroup_stats.conversion
        ,adgroup_stats.spend
        ,adgroup_stats.conversion_rate
        ,adgroup_stats.profile_visits
        ,adgroup_stats.cpc
        ,adgroup_stats.clicks
        ,adgroup_stats.likes
        ,adgroup_stats.reach
        ,adgroup_stats.impressions
        ,adgroup_stats.total_purchase_value
        ,adgroup_stats.total_purchase
        ,adgroup_stats.shares
        ,adgroup_history.updated_at
        ,adgroup_history.advertiser_id
        ,adgroup_history.campaign_id
        ,adgroup_history.create_time
        ,adgroup_history.adgroup_name
        ,adgroup_history.placement_type
        ,adgroup_history.external_action
        ,adgroup_history.creative_material_mode
        ,adgroup_history.gender
        ,adgroup_history.budget_mode
        ,adgroup_history.schedule_type
        ,adgroup_history.optimize_goal
        ,adgroup_history.pacing
        ,adgroup_history.billing_event
        ,adgroup_history.bid_type
        ,adgroup_history.adgroup_status
        ,adgroup_history.opt_status
        ,adgroup_history.video_download
        ,adgroup_history.budget
        ,adgroup_history.bid
        ,adgroup_history.conversion_bid
        ,adgroup_history.schedule_start_time
        ,adgroup_history.schedule_end_time
        ,adgroup_history.pixel_id
        ,adgroup_history.enable_inventory_filter
        ,adgroup_history.skip_learning_phase
        ,adgroup_history.adgroup_valid_from_date
        ,adgroup_history.adgroup_valid_to_date
        ,campaign_history.campaign_name
        ,campaign_history.campaign_type
    from adgroup_stats
        left join adgroup_history on adgroup_stats.adgroup_id = adgroup_history.adgroup_id
                                  and adgroup_stats.stat_at_date >= adgroup_history.adgroup_valid_from_date
                                  and adgroup_stats.stat_at_date < adgroup_history.adgroup_valid_to_date
        left join campaign_history on adgroup_history.campaign_id = campaign_history.campaign_id
                                   and adgroup_stats.stat_at_date >= campaign_history.campaign_valid_from_date
                                   and adgroup_stats.stat_at_date < campaign_history.campaign_valid_to_date
)

select *
from combined