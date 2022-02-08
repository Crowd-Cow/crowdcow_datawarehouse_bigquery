with 

source as ( select * from {{ source('google_ads', 'campaign_history') }} )

,renamed as (

    select
        id as campaign_id
        ,updated_at as updated_at_utc
        ,customer_id
        ,base_campaign_id
        ,{{ clean_strings('ad_serving_optimization_status') }} as ad_serving_optimization_status
        ,{{ clean_strings('advertising_channel_subtype') }} as advertising_channel_subtype
        ,{{ clean_strings('advertising_channel_type') }} as advertising_channel_type
        ,{{ clean_strings('experiment_type') }} as experiment_type
        ,end_date
        ,{{ clean_strings('final_url_suffix') }} as final_url_suffix
        ,frequency_caps
        ,{{ clean_strings('name') }} as campaign_name
        ,optimization_score
        ,{{ clean_strings('payment_mode') }} as payment_mode
        ,{{ clean_strings('serving_status') }} as serving_status
        ,start_date
        ,{{ clean_strings('status') }} as campaign_status
        ,{{ clean_strings('tracking_url_template') }} as tracking_url_template
        ,{{ clean_strings('vanity_pharma_display_url_mode') }} as vanity_pharma_display_url_mode
        ,{{ clean_strings('vanity_pharma_text') }} as vanity_pharma_text
        ,{{ clean_strings('video_brand_safety_suitability') }} as video_brand_safety_suitability

    from source

)

select * from renamed

