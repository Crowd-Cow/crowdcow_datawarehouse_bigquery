with

google_ads as ( select * from {{ ref('int_google_ads_ad_performance') }} )


select * from google_ads