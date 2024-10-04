{{ config(
  enabled=false
) }}
with 

ad_stats as (select * from {{ ref('stg_tiktok__ad_report_daily')}})
,ad_history as (select * from {{ ref('stg_tiktok__ad_history')}})
,adgroup_history as (select * from {{ ref('stg_tiktok__adgroup_history')}})
,campaign_history as (select * from {{ ref('stg_tiktok__campaign_history')}})

,combined as (
    select
        ad_stats.ad_id
        ,ad_history.adgroup_id
        ,adgroup_history.adgroup_name
        ,ad_history.campaign_id
        ,campaign_history.campaign_name
        ,ad_stats.stat_at_date
        ,ad_stats.video_views_p_100
        ,ad_stats.video_views_p_75
        ,ad_stats.video_views_p_50
        ,ad_stats.video_views_p_25
        ,ad_stats.video_play_actions
        ,ad_stats.video_watched_2_s
        ,ad_stats.video_watched_6_s
        ,ad_stats.profile_visits_rate
        ,ad_stats.follows
        ,ad_stats.ctr
        ,ad_stats.conversion
        ,ad_stats.spend
        ,ad_stats.conversion_rate
        ,ad_stats.profile_visits
        ,ad_stats.cpc
        ,ad_stats.clicks
        ,ad_stats.likes
        ,ad_stats.reach
        ,ad_stats.impressions
        ,ad_stats.total_purchase_value
        ,ad_stats.total_purchase
        ,ad_stats.shares
        ,ad_history.updated_at
        ,ad_history.create_time as ad_created_time
        ,ad_history.ad_name
        ,ad_history.call_to_action
        ,ad_history.ad_status
        ,ad_history.opt_status
        ,ad_history.ad_text
        ,ad_history.video_id
        ,ad_history.app_name
        ,ad_history.landing_page_url
        ,ad_history.display_name
        ,ad_history.profile_image
        ,ad_history.playable_url
        ,ad_history.is_aco
        ,ad_history.is_creative_authorized
        ,ad_history.is_new_structure
        ,ad_history.ad_valid_from_date
        ,ad_history.ad_valid_to_date
    from ad_stats
        left join ad_history on ad_stats.ad_id = ad_history.ad_id
    						 and ad_stats.stat_at_date >= ad_history.ad_valid_from_date
                             and ad_stats.stat_at_date < ad_history.ad_valid_to_date
        left join adgroup_history on ad_history.adgroup_id = adgroup_history.adgroup_id
    							  and ad_history.campaign_id = adgroup_history.campaign_id
                                  and ad_stats.stat_at_date >= adgroup_history.adgroup_valid_from_date
                                  and ad_stats.stat_at_date < adgroup_history.adgroup_valid_to_date
        left join campaign_history on ad_history.campaign_id = campaign_history.campaign_id
                                   and ad_stats.stat_at_date >= campaign_history.campaign_valid_from_date
                                   and ad_stats.stat_at_date < campaign_history.campaign_valid_to_date
)

select *
from combined