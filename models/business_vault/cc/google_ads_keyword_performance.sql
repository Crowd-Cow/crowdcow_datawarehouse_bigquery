with 

keyword_stats as (select * from {{ ref('stg_google_ads__keyword_stats')}})
,criterion_history as (select * from {{ ref('stg_google_ads__ad_group_criterion_history')}})
,campaign_history as (select * from {{ ref('stg_google_ads__campaign_history')}} )
,ad_group_history as (select * from {{ ref('stg_google_ads__ad_group_history')}})


,keyword_clicks_cost as (
    select 
        ad_group_criterion_criterion_id
        ,ad_group_id
        ,campaign_id
        ,occurred_at_date
        ,sum(clicks) as total_clicks
        ,sum(impressions) as total_impressions
        ,sum(cost_usd) as total_cost_usd
    from keyword_stats
    group by 1, 2, 3, 4
)

,criterion_details as (
    select 
        ad_group_criterion_id
        ,ad_group_id
        ,keyword_match_type
        ,keyword_text
        ,is_negative
        ,ad_group_criterion_valid_from_date
        ,ad_group_criterion_valid_to_date
    from criterion_history
)

,campaign_info as (
    select 
        campaign_id
        ,campaign_name
        ,campaign_valid_from_date
        ,campaign_valid_to_date
    from campaign_history
)

,ad_group_details as (
    select 
        ad_group_history.ad_group_id
        ,ad_group_history.ad_group_name
        ,ad_group_valid_from_date
        ,ad_group_valid_to_date
    from ad_group_history
)

select distinct 
    criterion_details.keyword_text
    ,criterion_details.keyword_match_type
    ,keyword_clicks_cost.ad_group_id
    ,keyword_clicks_cost.campaign_id
    ,keyword_clicks_cost.occurred_at_date
    ,keyword_clicks_cost.total_clicks
    ,keyword_clicks_cost.total_impressions
    ,keyword_clicks_cost.total_cost_usd
    ,campaign_info.campaign_name
    ,ad_group_details.ad_group_name
from keyword_clicks_cost
    left join criterion_details on keyword_clicks_cost.ad_group_criterion_criterion_id = criterion_details.ad_group_criterion_id
        and keyword_clicks_cost.ad_group_id = criterion_details.ad_group_id
        and keyword_clicks_cost.occurred_at_date >= criterion_details.ad_group_criterion_valid_from_date
        and keyword_clicks_cost.occurred_at_date < criterion_details.ad_group_criterion_valid_to_date
    left join campaign_info on keyword_clicks_cost.campaign_id = campaign_info.campaign_id
        and keyword_clicks_cost.occurred_at_date >= campaign_info.campaign_valid_from_date
        and keyword_clicks_cost.occurred_at_date < campaign_info.campaign_valid_to_date
    left join ad_group_details on keyword_clicks_cost.ad_group_id = ad_group_details.ad_group_id
        and keyword_clicks_cost.occurred_at_date >= ad_group_details.ad_group_valid_from_date
        and keyword_clicks_cost.occurred_at_date < ad_group_details.ad_group_valid_to_date