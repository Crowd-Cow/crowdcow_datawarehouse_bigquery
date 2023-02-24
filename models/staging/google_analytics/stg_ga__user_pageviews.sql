with

user_pageviews as ( select * from {{ source('google_analytics', 'user_pageviews') }} )

select
date::date as date
,{{ clean_strings('page_path') }} as page_path
,exits
,new_users
,avg_time_on_page
,bounces
,entrances
,users
,pageviews
,{{ clean_strings('device_category') }} as device_type
from user_pageviews