with visits as ( select * from {{ ref('visits') }} )
,users as ( select user_id, attributed_visit_id from {{ ref('users') }} )

,attribution_details as (
    select visits.visit_id
        ,users.user_id
        ,visits.utm_source
        ,visits.utm_medium
        ,visits.utm_campaign
        ,visits.utm_content
        ,visits.utm_term
        ,visits.channel
        ,visits.sub_channel
    from visits
    join users on users.attributed_visit_id = visits.visit_id
)

select * from attribution_details