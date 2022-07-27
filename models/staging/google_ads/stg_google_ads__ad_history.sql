with 

source as ( select * from {{ source('google_ads', 'ad_history') }} )

,renamed as (

    select
        id as ad_id
        ,ad_group_id
        ,updated_at as updated_at_utc
        ,updated_at::date as ad_valid_from_date
        ,ifnull(lead(updated_at::date,1) over(partition by id, ad_group_id order by updated_at),'2999-01-01') as ad_valid_to_date
        ,{{ clean_strings('action_items') }} as action_items
        ,{{ clean_strings('action_items') }} as ad_strength
        ,{{ clean_strings('device_preference') }} as device_preference
        ,{{ clean_strings('final_urls') }} as final_urls
        ,{{ clean_strings('name') }} as ad_name
        ,{{ clean_strings('policy_summary_approval_status') }} as policy_summary_approval_status
        ,{{ clean_strings('policy_summary_review_status') }} as policy_summary_review_status
        ,{{ clean_strings('status') }} as ad_status
        ,{{ clean_strings('system_managed_resource_source') }} as system_managed_resource_source
        ,{{ clean_strings('type') }} as ad_type
    from source
)

select * from renamed

