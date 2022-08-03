with 

source as ( select * from {{ source('cc', 'zendesk_tickets') }} where not _fivetran_deleted )

,renamed as (
    select
        id as zendesk_id
        ,zeroifnull(agent_wait_time_in_minutes) as agent_wait_time_in_minutes
        ,zd_updated_at as zd_updated_at_utc
        ,updated_at as update_at_utc
        ,{{ clean_strings('status') }} as zendesk_status
        ,ticket_id
        ,assignee_id
        ,{{ clean_strings('description') }} as ticket_description
        ,{{ clean_strings('priority') }} as priority
        ,created_at as created_at_utc
        ,{{clean_strings('via') }} as via
        ,{{ clean_strings('comments') }} as comments
        ,assignee_user_id
        ,zeroifnull(first_resolution_time_in_minutes) as first_resolution_time_in_minutes
        ,zeroifnull(reply_time_in_minutes) as reply_time_in_minutes
        ,requester_id
        ,{{ clean_strings('subject') }} as ticket_subject
        ,zd_solved_at as zd_solved_at_utc
        ,zeroifnull(requester_wait_time_in_minutes) as requester_wait_time_in_minutes
        ,{{ clean_strings('first_few_messages_flat') }} as first_few_messages_flat
        ,{{ clean_strings('ai_category') }} as ai_category
        ,zeroifnull(replies) as replies
        ,zeroifnull(reopens) as reopens
        ,{{ clean_strings('tags') }} as tags
        ,zd_created_at as zd_created_at_utc
        ,zeroifnull(full_resolution_time_in_minutes) as full_resolution_time_in_minutes
        ,user_id
        ,is_public
    from source
)

select * from renamed
