with 
email_campaign_events as ( select * from {{ ref('email_campaign_events') }} )

,emails as (
    SELECT 
        user_id
        ,campaign_name
        ,max(ended_at_utc)
        ,sum(send_count) as send_count
        ,sum(open_count) as open_count
        ,sum(click_count) as click_count
        ,sum(unique_open_count) as unique_open_count
        ,sum(bounce_count) as bounce_count
    from email_campaign_events 
    where
        campaign_type = 'TRIGGERED' 
    GROUP by 
        1,2 
)
select * from emails 