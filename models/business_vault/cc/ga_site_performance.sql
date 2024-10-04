{{ config(
  enabled=false
) }}
with

ga_site_performance as ( select * from {{ ref('stg_ga__site_performance') }} )

select * from ga_site_performance