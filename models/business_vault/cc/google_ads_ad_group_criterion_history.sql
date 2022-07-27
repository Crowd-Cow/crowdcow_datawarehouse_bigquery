with

ad_group_criterion_history as ( select * from {{ ref('stg_google_ads__ad_group_criterion_history') }} )

select * from ad_group_criterion_history