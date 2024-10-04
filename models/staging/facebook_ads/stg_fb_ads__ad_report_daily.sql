with 

source as ( select * from {{ source('facebook_ads', 'basic_ad_report_daily') }} )

,renamed as (

    select
        ad_id
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
        ,spend
        ,{{ clean_strings('ad_name') }} as ad_name
        ,{{ clean_strings('adset_name') }} as adset_name
        ,inline_link_click_ctr
    from source
)

select * from renamed