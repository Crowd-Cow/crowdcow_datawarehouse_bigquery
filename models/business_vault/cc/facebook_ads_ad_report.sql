{{ config(
  enabled=false
) }}

with

fb_ad_report as ( select * from {{ ref('stg_fb_ads__ad_report_daily') }} )

select * from fb_ad_report