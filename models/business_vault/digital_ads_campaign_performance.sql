with

google as ( select * from {{ ref('int_google_ads_campaign_performance') }} )

,combine_platforms as (
    select * from google
    /*** other digital adds platforms can be unioned here when ready ***/
)

select * from combine_platforms
