with

incoming_traffic as ( select * from {{ source('google_analytics', 'incoming_traffic') }} )

select
date::date as date
,{{ clean_strings('channel_grouping') }} as channel_grouping
,{{ clean_strings('landing_page_path') }} as landing_page_path
,{{ clean_strings('medium') }} as medium
,{{ clean_strings('source') }} as source
,new_users
,percent_new_visits
,percent_new_sessions
,users
from incoming_traffic