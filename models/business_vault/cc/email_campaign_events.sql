{{ config(
    materialized='incremental',
    unique_key='email_campaign_id',
    partition_by={'field': 'ended_at_utc', 'data_type': 'timestamp'},
    cluster_by=['email_campaign_id']
) }}

with

event as (
    select
        event_id,
        user_email,
        campaign_id,
        event_name,
        created_at_utc
    from {{ ref('stg_iterable__events') }}
    where event_name in ('EMAILSEND','EMAILOPEN','EMAILCLICK','EMAILBOUNCE')
      {% if is_incremental() %}
        and created_at_utc >= (select max(ended_at_utc) from {{ this }})
      {% endif %}
)
,campaign as ( 
    select 
        campaign_id
        ,campaign_name
        ,campaign_state
        ,created_by_user_id
        ,send_size
        ,workflow_id
        ,workflow_name
        ,campaign_type
        ,ended_at_utc
        ,list_name 
        from {{ ref('stg_iterable__campaign_history') }} )
,user as ( select user_email, user_token from {{ ref('stg_iterable__user_history') }} )
,cc_user as ( select distinct user_token, user_id from {{ ref('stg_cc__users') }} where dbt_valid_to is null )

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
        ,campaign.campaign_state
        ,event.event_name
        ,campaign.created_by_user_id
        ,campaign.send_size
        ,campaign.workflow_id
        ,campaign.workflow_name
        ,campaign.campaign_type
        ,campaign.list_name
        ,event.created_at_utc
        
        ,case
            when campaign.campaign_type = 'TRIGGERED' then event.created_at_utc
            else campaign.ended_at_utc
        end as ended_at_utc

    from event
        left join campaign on event.campaign_id = campaign.campaign_id
        left join get_user_id on event.user_email = get_user_id.user_email
)

,aggregate_campaigns as (
    select
        {{ dbt_utils.surrogate_key(['user_email','campaign_id','ended_at_utc']) }} as email_campaign_id
        ,user_email
        ,user_token
        ,user_id
        ,campaign_id
        ,campaign_name
        ,campaign_state
        ,created_by_user_id
        ,send_size
        ,workflow_id
        ,workflow_name
        ,campaign_type
        ,list_name
        ,ended_at_utc
        ,countif(event_name = 'EMAILSEND') as send_count
        ,countif(event_name = 'EMAILOPEN') as open_count
        ,countif(event_name = 'EMAILCLICK') as click_count
        ,count(distinct if(event_name = 'EMAILCLICK',user_id,null)) as unique_click_count
        ,count(distinct if(event_name = 'EMAILOPEN',user_id,null)) as unique_open_count
        ,countif(event_name = 'EMAILBOUNCE') as bounce_count
    from join_events_campaigns
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)

select
    email_campaign_id,
    user_email,
    user_token,
    user_id,
    campaign_id,
    campaign_name,
    campaign_state,
    created_by_user_id,
    send_size,
    workflow_id,
    workflow_name,
    campaign_type,
    list_name,
    ended_at_utc,
    send_count,
    open_count,
    click_count,
    unique_click_count,
    unique_open_count,
    bounce_count
from aggregate_campaigns
