{{ config(
  enabled=false
) }}
with 

source as ( select * from {{ source('google_ads', 'ad_group_history') }} )

,renamed as (

    select
        id as ad_group_id
        ,updated_at as updated_at_utc
        ,updated_at::date as ad_group_valid_from_date
        ,ifnull(lead(updated_at::date,1) over(partition by ad_group_id order by updated_at),'2999-01-01') as ad_group_valid_to_date
        ,campaign_id
        ,base_ad_group_id
        ,{{ clean_strings('ad_rotation_mode') }} as ad_rotation_mode
        ,{{ clean_strings('campaign_name') }} as campaign_name
        ,{{ clean_strings('display_custom_bid_dimension') }} as display_custom_bid_dimension
        ,explorer_auto_optimizer_setting_opt_in as is_explorer_auto_optimizer_setting_opt_in
        ,{{ clean_strings('name') }} as ad_group_name
        ,{{ clean_strings('status') }} as ad_group_status
        ,{{ clean_strings('type') }} as ad_group_type

    from source

)

select * from renamed

