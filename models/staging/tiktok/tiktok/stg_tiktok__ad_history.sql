{{ config(
  enabled=false
) }}
with

source as ( select * from {{ source('tiktok_ads', 'ad_history') }} )

,get_valid_from_to as (
    select
        ad_id
        ,updated_at
        ,advertiser_id
        ,adgroup_id
        ,campaign_id
        ,create_time
        ,{{ clean_strings('ad_name') }} as ad_name
        ,{{ clean_strings('call_to_action') }} as call_to_action
        ,{{ clean_strings('status') }} as ad_status
        ,{{ clean_strings('opt_status') }} as opt_status
        ,{{ clean_strings('ad_text') }} as ad_text
        ,{{ clean_strings('video_id') }} as video_id
        ,{{ clean_strings('app_name') }} as app_name
        ,{{ clean_strings('open_url') }} as open_url
        ,{{ clean_strings('landing_page_url') }} as landing_page_url
        ,{{ clean_strings('display_name') }} as display_name
        ,{{ clean_strings('profile_image') }} as profile_image
        ,{{ clean_strings('impression_tracking_url') }} as impression_tracking_url
        ,{{ clean_strings('click_tracking_url') }} as click_tracking_url
        ,{{ clean_strings('playable_url') }} as playable_url
        ,is_aco
        ,is_creative_authorized
        ,is_new_structure
        ,case
            when updated_at = first_value(updated_at) over(partition by ad_id order by updated_at) then create_time
            else updated_at
         end as ad_valid_from_date
        ,ifnull(lead(updated_at) over(partition by ad_id order by updated_at),'2999-01-01') as ad_valid_to_date
    from source
)

select *
from get_valid_from_to