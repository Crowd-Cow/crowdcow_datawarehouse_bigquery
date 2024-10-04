with

session_details as ( select * from {{ source('google_analytics_4', 'session_details') }} )

select
cast(date as date) as date
,newusers as new_users
,sessions as sessions
,averagesessionduration as average_session_duration
,transactions
,screenpageviewspersession as screen_page_views_per_session
,totalusers as total_users
,purchaserevenue as purchase_revenue
,{{ clean_strings('landingpage') }} as landing_page
,{{ clean_strings('landingpageplusquerystring') }} as landing_page_plus_query_string
,engagedsessions as engaged_sessions
,userengagementduration as user_engagement_duration

from session_details