 
with
ga4_user_pageviews as ( select * from {{ ref('stg_ga4__user_pageviews') }} )

select * from ga4_user_pageviews