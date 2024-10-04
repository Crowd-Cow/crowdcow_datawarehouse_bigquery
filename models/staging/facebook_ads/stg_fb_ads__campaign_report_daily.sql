with 

source as ( select * from {{ source('facebook_ads', 'basic_campaign_report_by_day') }} )

,renamed as (

    select
        campaign_id
        ,date::date as stat_date
        ,account_id
        ,impressions
        ,inline_link_clicks
        ,reach
        ,round(cost_per_inline_link_click,2) as cost_per_inline_link_click_usd
        ,round(cpc,2) as cpc_usd
        ,cpm
        ,ctr
        ,frequency
        ,spend as spend_usd
        ,{{ clean_strings('campaign_name') }} as campaign_name
        ,inline_link_click_ctr
    from source
)

select * from renamed