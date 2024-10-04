{{ config(
  enabled=false
) }}
with

source as ( select * from {{ source('tiktok_ads', 'adgroup_history') }} )

,get_valid_from_to as (
    select
        adgroup_id
        ,updated_at
        ,advertiser_id
        ,campaign_id
        ,create_time
        ,{{ clean_strings('adgroup_name') }} as adgroup_name
        ,{{ clean_strings('placement_type') }} as placement_type
        ,{{ clean_strings('external_action') }} as external_action
        ,{{ clean_strings('creative_material_mode') }} as creative_material_mode
        ,{{ clean_strings('gender') }} as gender
        ,{{ clean_strings('budget_mode') }} as budget_mode
        ,{{ clean_strings('schedule_type') }} as schedule_type
        ,{{ clean_strings('optimize_goal') }} as optimize_goal
        ,{{ clean_strings('pacing') }} as pacing
        ,{{ clean_strings('billing_event') }} as billing_event
        ,{{ clean_strings('bid_type') }} as bid_type
        ,{{ clean_strings('status') }} as adgroup_status
        ,{{ clean_strings('opt_status') }} as opt_status
        ,{{ clean_strings('video_download') }} as video_download
        ,budget
        ,bid
        ,conversion_bid
        ,deep_cpabid
        ,schedule_start_time
        ,schedule_end_time
        ,pixel_id
        ,enable_inventory_filter
        ,is_hfss
        ,is_new_structure
        ,category
        ,is_comment_disable
        ,skip_learning_phase
        ,location
        ,interest_category_v_2
        ,placement
        ,age
        ,languages
        ,case
            when updated_at = first_value(updated_at) over(partition by adgroup_id order by updated_at) then create_time
            else updated_at
         end as adgroup_valid_from_date
        ,ifnull(lead(updated_at) over(partition by adgroup_id order by updated_at),'2999-01-01') as adgroup_valid_to_date
    from source
)

select *
from get_valid_from_to