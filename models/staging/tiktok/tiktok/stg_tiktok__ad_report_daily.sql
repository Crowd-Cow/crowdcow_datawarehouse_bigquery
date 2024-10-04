with

source as ( select * from {{ source('tiktok_ads', 'ad_report_daily') }} )

,renamed as (
    select 
        ad_id
        ,stat_time_day::date as stat_at_date
        ,video_views_p_100
        ,video_views_p_75
        ,video_views_p_50
        ,video_views_p_25
        ,video_play_actions
        ,video_watched_2_s
        ,video_watched_6_s
        ,profile_visits_rate
        ,follows
        ,ctr
        ,conversion
        ,spend
        ,conversion_rate
        ,profile_visits
        ,cpc
        ,clicks
        ,likes
        ,reach
        ,impressions
        ,total_purchase_value
        ,total_purchase
        ,shares
    from source
)

select * from renamed