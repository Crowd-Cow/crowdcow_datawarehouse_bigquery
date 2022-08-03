with

tickets as ( select * from {{ ref('stg_cc__zendesk_tickets') }} )
,sat_rating as ( select * from {{ ref('stg_zendesk__satisfaction_ratings') }} )
,zendesk_user as ( select * from {{ ref('stg_zendesk__users') }} )

,dedup_ratings as (
    select
        assignee_id as zd_agent_id
        ,csat_score
        ,updated_at_utc as csat_received_at_utc
        ,ticket_id
    from sat_rating
    qualify row_number() over(partition by ticket_id order by created_at_utc desc) = 1    
)

,get_ratings_users as (
    select
        tickets.*
        ,dedup_ratings.zd_agent_id
        ,dedup_ratings.csat_score
        ,dedup_ratings.csat_received_at_utc
        ,zendesk_user.is_active as is_agent_active
        ,zendesk_user.user_alias as agent_alias
        ,zendesk_user.user_name as agent_name
        ,zendesk_user.user_email as agent_email
        ,requester.is_active as is_requester_active
        ,requester.created_at_utc as requester_created_at_utc
        ,requester.user_name as requester_name
        ,requester.user_email as requester_email
    from tickets
        left join dedup_ratings on tickets.ticket_id = dedup_ratings.ticket_id
        left join zendesk_user on tickets.assignee_id = zendesk_user.user_id
        left join zendesk_user as requester on tickets.requester_id = requester.user_id
)

select * from get_ratings_users
