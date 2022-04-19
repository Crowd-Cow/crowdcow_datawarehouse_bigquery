with

event as ( select * from {{ ref('stg_iterable__events') }} )
,campaign as ( select * From {{ ref('stg_iterable__campaign_history') }} )
,user as ( select * from {{ ref('stg_iterable__user_history') }} )
,cc_user as ( select * from {{ ref('stg_cc__users') }} where dbt_valid_to is null )

,get_user_id as (
    select 
        user.user_email
        ,user.user_token
        ,cc_user.user_id
    from user
        left join cc_user on user.user_token = cc_user.user_token
)

,join_events_campaigns as (
    select
        event.event_id
        ,event.user_email
        ,get_user_id.user_token
        ,get_user_id.user_id
        ,event.campaign_id
        ,campaign.campaign_name
        ,event.event_name
        ,campaign.created_by_user_id
        ,campaign.send_size
        ,campaign.campaign_type
        ,event.created_at_utc
        ,campaign.ended_at_utc
    from event
        left join campaign on event.campaign_id = campaign.campaign_id
        left join get_user_id on event.user_email = get_user_id.user_email
)

select * from join_events_campaigns
