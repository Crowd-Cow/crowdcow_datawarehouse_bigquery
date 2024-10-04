{{ config(
  enabled=false
) }}
with

session_details as ( select * from {{ source('google_analytics', 'session_details') }} )

select
date::date as date
,new_users
,sessions
,avg_session_duration
,transaction_revenue
,pageviews_per_session
,users
from session_details