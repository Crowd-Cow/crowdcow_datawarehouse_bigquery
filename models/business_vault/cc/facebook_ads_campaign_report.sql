with

fb_campaign_report as ( select * from {{ ref('stg_fb_ads__campaign_report_daily') }} )

select * from fb_campaign_report