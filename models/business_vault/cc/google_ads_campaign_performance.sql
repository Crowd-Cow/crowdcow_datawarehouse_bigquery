with  

campaign_stats as (select * from {{ ref('stg_google_ads__campaign_performance')}})

select * from campaign_stats