{{ config(
  enabled=false
) }}
with

ga_user_pageviews as ( select * from {{ ref('stg_ga__user_pageviews') }} )

select * from ga_user_pageviews