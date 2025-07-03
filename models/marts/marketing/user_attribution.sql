{{ config(
    materialized='incremental',
    unique_key='visit_id',
    partition_by={'field': 'started_at_utc', 'data_type': 'timestamp'},
    cluster_by=['utm_campaign','channel'],
    on_schema_change = 'sync_all_columns'
) }}

with
visits as (
  select
    visit_id,
    started_at_utc,
    cast(started_at_utc as date) as started_at_utc_date,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_term,
    channel,
    sub_channel,
    visit_landing_page,
    visit_landing_page_path,
    gclid,
    is_prospect
  from {{ ref('visits') }}
  {% if is_incremental() %}
    where started_at_utc >= (select max(started_at_utc) from {{ this }})
  {% endif %}
)
,users as ( select user_id, attributed_visit_id from {{ ref('users') }} )

,attribution_details as (
    select visits.visit_id
        ,visits.started_at_utc
        ,cast(visits.started_at_utc as date) as started_at_utc_date
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
        ,visits.is_prospect
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
    ,attribution_details.is_prospect
    ,{{ dbt_utils.surrogate_key( ['started_at_utc_date','campaign_grouping'] ) }} as campaign_key
from attribution_details
