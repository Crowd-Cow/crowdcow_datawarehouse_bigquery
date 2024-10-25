with

incoming_traffic as ( select * from {{ source('google_analytics_4', 'incoming_traffic') }} )

select
date(date) as date
,{{ clean_strings('SESSIONDEFAULTCHANNELGROUP') }} as channel_grouping
,{{ clean_strings('LANDINGPAGE') }} as landing_page_path
,{{ clean_strings('SESSIONMEDIUM') }} as medium
,{{ clean_strings('SESSIONSOURCE') }} as source
,NEWUSERS as new_users
,TOTALUSERS as  users
,ACTIVEUSERS as active_users
from incoming_traffic