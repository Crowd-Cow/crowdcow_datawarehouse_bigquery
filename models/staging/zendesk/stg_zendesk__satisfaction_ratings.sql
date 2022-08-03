with

source as ( select * from {{ source('zendesk', 'satisfaction_rating') }} )

,renamed as (
    select
        id as zendesk_satisfaction_rating_id
        ,{{ clean_strings('url') }} as zendesk_url
        ,assignee_id
        ,group_id
        ,requester_id
        ,ticket_id
        ,{{ clean_strings('score') }} as csat_score
        ,created_at as created_at_utc
        ,updated_at as updated_at_utc
        ,{{ clean_strings('comment') }} as comment
        ,{{ clean_strings('reason') }} as reason
    from source
)

select * from renamed
