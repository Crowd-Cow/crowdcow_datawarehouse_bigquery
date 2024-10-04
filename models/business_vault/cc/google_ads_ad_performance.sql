{{ config(
  enabled=false
) }}
with  

ad_stats as (select * from {{ ref('stg_google_ads__ad_stats')}})
,ad_history as (select * from {{ ref('stg_google_ads__ad_history')}})
,campaign_history as (select * from {{ ref('stg_google_ads__campaign_history')}} )
,ad_group_history as (select * from {{ ref('stg_google_ads__ad_group_history')}})
,ad_custom_parameter_history as (select * from {{ ref('stg_google_ads__ad_custom_parameter_history')}})
,ad_group_custom_parameter_history as (select * from {{ ref('stg_google_ads__ad_group_custom_parameter_history')}})

,ad_clicks_cost as (
    select 
        ad_id
        ,ad_group_id
        ,campaign_id
        ,date_utc
        ,sum(clicks) as total_clicks
        ,sum(impressions) as total_impressions
        ,sum(cost_usd) as total_cost_usd
        ,sum(conversions) as total_conversions
        ,sum(conversions_value) as total_conversion_value
    from ad_stats
    group by 1, 2, 3, 4
)

,ad_url as (
    select 
        ad_id
        ,ad_group_id
        ,trim(trim(final_urls,']'),'[') as final_url
        ,ad_valid_from_date as ad_url_valid_from_date
        ,ad_valid_to_date as ad_url_valid_to_date
    from ad_history
)

,ad_custom_parameters as (
    select 
        ad_group_id
        ,ad_id
        ,value
        ,parameter_valid_from_date
        ,parameter_valid_to_date
    from ad_custom_parameter_history
)

,campaign_info as (
    select 
        campaign_id
        ,campaign_name

        ,case 
            when campaign_name like '%SHOPPING%' then 'SHOPPING' 
            else campaign_name 
         end as campaign_grouping
        
        ,campaign_valid_from_date
        ,campaign_valid_to_date
    from campaign_history
)

,ad_group_details as (
    select 
        ad_group_history.ad_group_id
        ,ad_group_history.ad_group_name
        ,ad_group_custom_parameter_history.ad_group_parameter
        ,ad_group_custom_parameter_history.ad_group_id_parameter
        ,ad_group_valid_from_date
        ,ad_group_valid_to_date
    from ad_group_history
        left join ad_group_custom_parameter_history on ad_group_history.ad_group_id = ad_group_custom_parameter_history.ad_group_id
                                                    and ad_group_history.updated_at_utc = ad_group_custom_parameter_history.updated_at_utc
)

select distinct 
    ad_clicks_cost.ad_id
    ,ad_clicks_cost.ad_group_id
    ,ad_group_id_parameter
    ,ad_clicks_cost.campaign_id
    ,ad_clicks_cost.date_utc
    ,ad_clicks_cost.total_clicks
    ,ad_clicks_cost.total_impressions
    ,ad_clicks_cost.total_cost_usd
    ,ad_clicks_cost.total_conversions
    ,ad_clicks_cost.total_conversion_value
    ,ad_url.final_url
    ,campaign_info.campaign_name
    ,ad_group_details.ad_group_name
    ,ad_group_parameter
    ,ad_custom_parameters.value
    ,{{ dbt_utils.surrogate_key( ['ad_clicks_cost.date_utc','campaign_grouping'] ) }} as campaign_key
from ad_clicks_cost
    left join ad_url on ad_clicks_cost.ad_id = ad_url.ad_id
        and ad_clicks_cost.ad_group_id = ad_url.ad_group_id
        and ad_clicks_cost.date_utc >= ad_url.ad_url_valid_from_date
        and ad_clicks_cost.date_utc < ad_url.ad_url_valid_to_date
    left join campaign_info on ad_clicks_cost.campaign_id = campaign_info.campaign_id
        and ad_clicks_cost.date_utc >= campaign_info.campaign_valid_from_date
        and ad_clicks_cost.date_utc < campaign_info.campaign_valid_to_date
    left join ad_group_details on ad_clicks_cost.ad_group_id = ad_group_details.ad_group_id
        and ad_clicks_cost.date_utc >= ad_group_details.ad_group_valid_from_date
        and ad_clicks_cost.date_utc < ad_group_details.ad_group_valid_to_date
    left join ad_custom_parameters on ad_clicks_cost.ad_id = ad_custom_parameters.ad_id
        and ad_clicks_cost.ad_group_id = ad_custom_parameters.ad_group_id
        and ad_clicks_cost.date_utc >= ad_custom_parameters.parameter_valid_from_date
        and ad_clicks_cost.date_utc < ad_custom_parameters.parameter_valid_to_date