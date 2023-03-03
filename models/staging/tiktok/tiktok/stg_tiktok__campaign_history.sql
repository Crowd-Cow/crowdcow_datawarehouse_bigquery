with

source as ( select * from {{ source('tiktok_ads', 'campaign_history') }} )

,get_valid_from_to as (
    select
        campaign_id
        ,updated_at
        ,advertiser_id
        ,{{ clean_strings('campaign_name') }} as campaign_name
        ,{{ clean_strings('campaign_type') }} as campaign_type
        ,budget as budget_usd
        ,{{ clean_strings('budget_mode') }} as budget_mode
        ,{{ clean_strings('opt_status') }} as opt_status
        ,{{ clean_strings('objective_type') }} as objective_type
        ,{{ clean_strings('is_new_structure') }} as is_new_structure
        ,{{ clean_strings('split_test_variable') }} as split_test_variable
        ,{{ clean_strings('status') }} as  campaign_status
        ,create_time
        ,case
            when updated_at = first_value(updated_at) over(partition by campaign_id order by updated_at) then create_time
            else updated_at
         end as campaign_valid_from_date
        ,ifnull(lead(updated_at) over(partition by campaign_id order by updated_at),'2999-01-01') as campaign_valid_to_date
    from source
)

select *
from get_valid_from_to