with

zd_tickets as (select * from {{ ref('stg_zendesk__tickets')}})
,cc_tickets as ( select * from {{ ref('stg_cc__zendesk_tickets') }} )
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

,tickets as (
    select
        zd_tickets.ticket_id
        ,zd_tickets.ticket_priority
        ,zd_tickets.ticket_status
        ,zd_tickets.ticket_subject
        ,zd_tickets.automation_type
        ,zd_tickets.cow_cash_reason
        ,zd_tickets.custom_type
        ,zd_tickets.due_at_utc
        ,zd_tickets.issue_category
        ,zd_tickets.ai_category
        ,zd_tickets.assignee_id
        ,zd_tickets.created_at_utc
        ,zd_tickets.updated_at_utc
        ,zd_tickets.is_public
        ,zd_tickets.merged_ticket_ids
        ,zd_tickets.order_errors
        ,zd_tickets.order_token
        ,zd_tickets.requester_id
        ,zd_tickets.ticket_description
        ,zd_tickets.ticket_form_order_number
        ,zd_tickets.via_channel
        ,zd_tickets.stylo_frustration_options
        ,zd_tickets.stylo_urgency
        ,zd_tickets.stylo_de_escalate
        ,zd_tickets.stylo_p_csat
        ,zd_tickets.stylo_peak_delight
        ,zd_tickets.stylo_frustration_change
        ,zd_tickets.stylo_delight
        ,zd_tickets.stylo_urgency_change
        ,zd_tickets.stylo_frustration_change_numeric
        ,zd_tickets.stylo_peak_agent_frustration
        ,zd_tickets.stylo_delight_change_numeric
        ,zd_tickets.stylo_urgency_change_numeric
        ,zd_tickets.stylo_frustration
        ,zd_tickets.stylo_peak_urgency
        ,zd_tickets.stylo_peak_frustration
        ,zd_tickets.stylo_urgency_options
        ,zd_tickets.ask_stylo_issue_category
        ,cc_tickets.agent_wait_time_in_minutes
        ,cc_tickets.assignee_user_id
        ,cc_tickets.comments
        ,cc_tickets.first_few_messages_flat
        ,cc_tickets.first_resolution_time_in_minutes
        ,cc_tickets.full_resolution_time_in_minutes
        ,cc_tickets.priority
        ,cc_tickets.reopens
        ,cc_tickets.replies
        ,cc_tickets.reply_time_in_minutes
        ,cc_tickets.requester_wait_time_in_minutes
        ,cc_tickets.tags
        ,cc_tickets.user_id
        ,cc_tickets.zd_solved_at_utc
        ,cc_tickets.reopens > 0 as is_ticket_reopened
        ,cc_tickets.ticket_id is not null as is_ticket_in_cc_zendesk
        ,zd_tickets.issue_category like 'ORDER_HELD%' or zd_tickets.ticket_subject like 'ORDER BEING HELD:%' as is_order_held_ticket
        ,array_size(zd_tickets.merged_ticket_ids) > 0 as is_merged_ticket
    from zd_tickets
        left join cc_tickets on zd_tickets.ticket_id = cc_tickets.ticket_id
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
