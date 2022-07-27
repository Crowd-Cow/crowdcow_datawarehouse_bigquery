with

ad_history as ( select * from {{ ref('stg_google_ads__ad_history') }} )

select * from ad_history