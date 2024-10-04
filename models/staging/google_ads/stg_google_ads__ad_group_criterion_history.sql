{{ config(
  enabled=false
) }}
with 

source as ( select * from {{ source('google_ads', 'ad_group_criterion_history') }} )

,renamed as (
    select 
        ad_group_id
        ,id as ad_group_criterion_id
        ,updated_at as uptdated_at_utc
        ,updated_at::date as ad_group_criterion_valid_from_date
        ,ifnull(lead(updated_at::date,1) over(partition by id, ad_group_id order by updated_at),'2999-01-01') as ad_group_criterion_valid_to_date
        ,{{ clean_strings('approval_status') }} as ad_group_criterion_approval_status
        ,bid_modifier
        ,{{ clean_strings('display_name') }} as display_name
        ,{{ clean_strings('final_urls') }} as final_urls
        ,first_page_cpc_micros*power(10,-6) as first_page_cpc_usd
        ,first_position_cpc_micros*power(10,-6) as first_position_cpc_usd
        ,{{ clean_strings('gender_type') }} as gender_type
        ,{{ clean_strings('income_range_type') }} income_range_type
        ,{{ clean_strings('keyword_match_type') }} as keyword_match_type
        ,{{ clean_strings('keyword_text') }} as keyword_text
        ,negative as is_negative
        ,quality_info_score as quality_info_score
        ,{{ clean_strings('quality_info_creative_score') }} as quality_info_creative_score
        ,{{ clean_strings('quality_info_search_predicted_ctr') }} as quality_info_search_predicted_ctr
        ,{{ clean_strings('status') }} as status
        ,{{ clean_strings('system_serving_status') }} as system_serving_status
        ,cpc_bid_micros*power(10,-6) as cpc_bid_usd
    from source
)

select * from renamed