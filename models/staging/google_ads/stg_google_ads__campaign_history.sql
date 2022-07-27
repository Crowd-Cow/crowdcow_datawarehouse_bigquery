with 

/**** Google Ads changed their API in April 2022. This Staging model cmobines the old historical API with the new API. ****/

source as ( select * from {{ source('google_ads', 'campaign_history') }} )
,old_adwords as ( select * from {{ source('adwords', 'campaign_history') }} )

,renamed as (

    select
        id as campaign_id
        ,updated_at as updated_at_utc
        ,updated_at::date as updated_at_date
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

    union all

    select
        id
        ,updated_at as updated_at_utc
        ,updated_at::date as updated_at_date
        ,customer_id
        ,base_campaign_id
        ,{{ clean_strings('ad_serving_optimization_status') }} as ad_serving_optimization_status
        ,{{ clean_strings('advertising_channel_subtype') }} as advertising_channel_subtype
        ,{{ clean_strings('advertising_channel_type') }} as advertising_channel_type
        ,null::text as experiment_type
        ,end_date
        ,{{ clean_strings('final_url_suffix') }} as final_url_suffix
        ,null::text as frequency_caps
        ,{{ clean_strings('name') }} as campaign_name
        ,null::float as optimization_score
        ,null::text as payment_mode
        ,{{ clean_strings('serving_status') }} as serving_status
        ,start_date
        ,{{ clean_strings('status') }} as campaign_status
        ,{{ clean_strings('tracking_url_template') }} as tracking_url_template
        ,{{ clean_strings('vanity_pharma_display_url_mode') }} as vanity_pharma_display_url_mode
        ,{{ clean_strings('vanity_pharma_text') }} as vanity_pharma_text
        ,null::text as video_brand_safety_suiability
    from old_adwords

)

,dedup_history as (
    select
        *
    from renamed
    qualify row_number() over(partition by campaign_id, updated_at_date order by updated_at_utc desc) = 1
)

,get_snapshot_dates as (
    select 
        campaign_id
        ,customer_id
        ,base_campaign_id
        ,ad_serving_optimization_status
        ,advertising_channel_subtype
        ,advertising_channel_type
        ,experiment_type
        ,end_date
        ,final_url_suffix
        ,frequency_caps
        ,campaign_name
        ,optimization_score
        ,payment_mode
        ,serving_status
        ,start_date
        ,campaign_status
        ,tracking_url_template
        ,vanity_pharma_display_url_mode
        ,vanity_pharma_text
        ,video_brand_safety_suitability
        ,updated_at_utc
        ,{{ dbt_utils.surrogate_key(['campaign_id','updated_at_date']) }} as history_key

        ,case
            when updated_at_date = first_value(updated_at_date) over(partition by campaign_id order by updated_at_date) then '1970-01-01'
            else updated_at_date
         end as campaign_valid_from_date
        
        ,ifnull(lead(updated_at_date) over(partition by campaign_id order by updated_at_date),'2999-01-01') as campaign_valid_to_date
    from dedup_history
)

select * from get_snapshot_dates
