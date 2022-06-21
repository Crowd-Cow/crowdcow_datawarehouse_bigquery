with 

promotion as ( select * from {{ ref('promotions_ss') }} where not _fivetran_deleted )
,promotion_promotion as ( select * from {{ ref('promotions_promotions_ss') }} where not _fivetran_deleted )

,union_promotions as (
    select
    'PROMOTION' as promotion_source
    ,id
    ,updated_at
    ,null::timestamp as starts_at
    ,null::int as claimable_window_in_days
    ,null::boolean as must_be_claimed
    ,null::timestamp as ends_at
    ,promotion_type
    ,created_at
    ,always_available
    ,must_be_assigned_to_user
    ,must_be_assigned_to_order
    ,null::boolean must_be_applied_by_user
    ,dbt_scd_id
    ,dbt_updated_at
    ,dbt_valid_from
    ,dbt_valid_to
from promotion

union all

select  
    'PROMOTIONS::PROMOTION' as promotion_source
    ,id
    ,updated_at
    ,starts_at
    ,claimable_window_in_days
    ,must_be_claimed
    ,ends_at
    ,name as promotion_type
    ,created_at
    ,null::boolean as always_available
    ,null::boolean as must_be_assigned_to_user
    ,null::boolean as must_be_assigned_to_order
    ,must_be_claimed
    ,dbt_scd_id
    ,dbt_updated_at
    ,dbt_valid_from
    ,dbt_valid_to
from promotion_promotion
)

,renamed as (

    select
        id as promotion_id
        ,promotion_source
        ,dbt_scd_id as promotion_key
        ,{{ clean_strings('promotion_type') }} as promotion_type
        ,always_available as is_always_available
        ,must_be_assigned_to_user
        ,must_be_assigned_to_order
        ,claimable_window_in_days
        ,must_be_claimed
        ,must_be_applied_by_user
        ,starts_at as starts_at_utc
        ,ends_at as ends_at_utc
        ,created_at as created_at_utc
        ,updated_at as updated_at_utc
        ,dbt_valid_to
        ,dbt_valid_from
        ,case
            when dbt_valid_from = first_value(dbt_valid_from) over(partition by id order by dbt_valid_from) then '1970-01-01'
            else dbt_valid_from
        end as adjusted_dbt_valid_from
        ,coalesce(dbt_valid_to,'2999-01-01') as adjusted_dbt_valid_to

    from union_promotions

)

select * from renamed
