with 
email_campaign_events as ( select * from {{ ref('email_campaign_events') }} )


,max_ended AS (
    SELECT 
        user_id,
        campaign_name,
        MAX(ended_at_utc) AS max_ended_at_utc
    FROM email_campaign_events 
    WHERE campaign_type = 'TRIGGERED' 
    GROUP BY user_id, campaign_name
)
,final as (
    SELECT 
        ROW_NUMBER() OVER(ORDER BY max_ended.user_id, max_ended.campaign_name, max_ended.max_ended_at_utc) AS prim_key,
        max_ended.user_id,
        max_ended.campaign_name,
        max_ended_at_utc as ended_at_utc,
        SUM(send_count) AS send_count,
        SUM(open_count) AS open_count,
        SUM(click_count) AS click_count,
        SUM(unique_open_count) AS unique_open_count,
        SUM(bounce_count) AS bounce_count
    FROM max_ended
    JOIN email_campaign_events AS events 
        ON max_ended.user_id = events.user_id 
        AND max_ended.campaign_name = events.campaign_name 
        AND max_ended.max_ended_at_utc = events.ended_at_utc
    GROUP BY max_ended.user_id, max_ended.campaign_name, max_ended.max_ended_at_utc
    ORDER BY max_ended.user_id, max_ended.campaign_name, max_ended.max_ended_at_utc
)

select * from final 