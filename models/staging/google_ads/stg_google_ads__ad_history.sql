with 

source as ( select * from {{ source('google_ads', 'ad_history') }} )
,old_adwords as ( select * from {{ source('adwords', 'ad_history') }} )

,renamed as (

    select
        id as ad_id
        ,ad_group_id
        ,updated_at as updated_at_utc
        ,updated_at::date as updated_at_date
        {# ,ifnull(lead(updated_at::date,1) over(partition by id, ad_group_id order by updated_at),'2999-01-01') as ad_valid_to_date #}
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

    union all

    select
        id as ad_id
        ,ad_group_id
        ,updated_at as updated_at_utc
        ,updated_at::date as updated_at_date
        ,null::text as action_items
        ,null::text as ad_strength
        ,{{ clean_strings('device_preference') }} as device_preference
        ,null::text as final_urls
        ,null::text as ad_name
        ,{{ clean_strings('policy_summary_combined_approval_status') }} as policy_summary_approval_status
        ,{{ clean_strings('policy_summary_review_state') }} as policy_summary_review_status
        ,{{ clean_strings('status') }} as ad_status
        ,{{ clean_strings('system_managed_entity_source') }} as system_managed_resource_source
        ,null::text as ad_type
    from old_adwords
)

,dedup_history as (
    select
        *
    from renamed
    qualify row_number() over(partition by ad_id,ad_group_id,updated_at_date order by updated_at_utc desc) = 1
)

,get_snapshot_dates as (
    select
        ad_id
        ,ad_group_id
        ,updated_at_utc
        ,action_items
        ,ad_strength
        ,device_preference
        ,final_urls
        ,ad_name
        ,policy_summary_approval_status
        ,policy_summary_review_status
        ,ad_status
        ,system_managed_resource_source
        ,ad_type
        ,{{ dbt_utils.surrogate_key(['ad_id','ad_group_id','updated_at_date']) }} as history_key

        ,case
            when updated_at_date = first_value(updated_at_date) over(partition by ad_id, ad_group_id order by updated_at_date) then '1970-01-01'
            else updated_at_date
         end as ad_valid_from_date
        
        ,ifnull(lead(updated_at_date) over(partition by ad_id,ad_group_id order by updated_at_date),'2999-01-01') as ad_valid_to_date
    from dedup_history
)

select * from get_snapshot_dates
