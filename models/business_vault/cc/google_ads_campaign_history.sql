with

campaign_history as ( select * from {{ ref('stg_google_ads__campaign_history') }} )

select * from campaign_history