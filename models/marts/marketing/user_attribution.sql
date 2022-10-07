with 

visits as ( select * from {{ ref('visits') }} )
,users as ( select user_id, attributed_visit_id from {{ ref('users') }} )

,attribution_details as (
    select visits.visit_id
        ,visits.started_at_utc
        ,visits.started_at_utc::date as started_at_utc_date
        ,users.user_id
        ,visits.utm_source
        ,visits.utm_medium
        ,visits.utm_campaign
        ,case when visits.utm_campaign like '%SHOPPING%' then 'SHOPPING' else visits.utm_campaign end as campaign_grouping
        ,visits.utm_content
        ,visits.utm_term
        ,visits.channel
        ,visits.sub_channel
        ,visits.visit_landing_page
        ,visits.visit_landing_page_path
        ,visits.gclid
    from visits
        join users on users.attributed_visit_id = visits.visit_id
)

select 
    attribution_details.visit_id
    ,attribution_details.started_at_utc
    ,attribution_details.user_id
    ,attribution_details.utm_source
    ,attribution_details.utm_medium
    ,attribution_details.utm_campaign
    ,attribution_details.utm_content
    ,attribution_details.utm_term
    ,attribution_details.channel
    ,attribution_details.sub_channel
    ,attribution_details.visit_landing_page
    ,attribution_details.visit_landing_page_path
    ,attribution_details.gclid
    ,{{ dbt_utils.surrogate_key( ['started_at_utc_date','campaign_grouping'] ) }} as campaign_key
from attribution_details
