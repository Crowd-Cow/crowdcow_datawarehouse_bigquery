with 

user_pageviews as ( select * from {{ source('google_analytics_4', 'user_page_views') }} )

select
cast(date as date) as date
,{{ clean_strings('pagepath') }} as page_path
,newusers as new_users
,userengagementduration as user_engagement_duration
,sessions - engagedsessions as bounces
,engagedsessions as engaged_sessions
,sessions as entrances
,totalusers as users
,screenpageviews as pageviews
,{{ clean_strings('platformdevicecategory') }} as device_type
from user_pageviews