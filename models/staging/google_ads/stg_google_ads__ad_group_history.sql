with 

source as ( select * from {{ source('google_ads', 'ad_group_history') }} )

,renamed as (

    select
        id as ad_group_id
        ,updated_at as updated_at_utc
        ,campaign_id
        ,base_ad_group_id
        ,{{ clean_strings('ad_rotation_mode') }} as ad_rotation_mode
        ,{{ clean_strings('campaign_name') }} as campaign_name
        ,{{ clean_strings('display_custom_bid_dimension') }} as display_custom_bid_dimension
        ,{{ clean_strings('explorer_auto_optimizer_setting_opt_in') }} as explorer_auto_optimizer_setting_opt_in
        ,{{ clean_strings('final_url_suffix') }} as final_url_suffix
        ,{{ clean_strings('name') }} as ad_group_name
        ,{{ clean_strings('status') }} as ad_group_status
        ,{{ clean_strings('tracking_url_template') }} as tracking_url_template
        ,{{ clean_strings('type') }} as ad_group_type

    from source

)

select * from renamed

