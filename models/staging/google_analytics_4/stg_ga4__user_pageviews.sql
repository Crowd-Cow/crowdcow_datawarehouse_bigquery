with

user_pageviews as ( select * from {{ source('google_analytics_4', 'user_pageviews') }} )

select
date::date as date
,{{ clean_strings('page_path') }} as page_path
,new_users
,user_engagement_duration
,sessions - engaged_sessions as bounces
,engaged_sessions 
,sessions as entrances
,total_users as users
,screen_page_views as pageviews
,{{ clean_strings('device_category') }} as device_type
from user_pageviews