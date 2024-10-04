with 

promotion as ( select * from {{ ref('promotions_ss') }}  )
,promotion_promotion as ( select * from {{ ref('promotions_promotions_ss') }}   )
,promotion_code as (
    select
        configurable_id as promotion_id
        ,promotion_configuration_value as promo_code
        ,configurable_type as promotion_source
        ,concat(promotion_configuration_key,'_',promotion_configuration_value) as promotion_key_value
    from {{ ref('stg_cc__promotions_configurations') }}
    where promotion_configuration_key in ('PROMO_CODE','REWARDS_PROGRAM')
    and configurable_type = 'PROMOTIONS::PROMOTION'
    and promotion_configuration_value <> 'GIFT4DAD2023'
)

,union_promotions as (
    select
    'PROMOTION' as promotion_source
    ,id
    ,updated_at
    ,timestamp(null) as starts_at
    ,INT64(null) as claimable_window_in_days
    ,INT64(null) as must_be_claimed
    ,timestamp(null) as ends_at
    ,promotion_type
    ,created_at
    ,always_available
    ,must_be_assigned_to_user
    ,must_be_assigned_to_order
    ,INT64(null) must_be_applied_by_user
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
    ,INT64(null) as always_available
    ,INT64(null) as must_be_assigned_to_user
    ,INT64(null) as must_be_assigned_to_order
    ,must_be_claimed
    ,dbt_scd_id
    ,dbt_updated_at
    ,dbt_valid_from
    ,dbt_valid_to
from promotion_promotion
)

,renamed as (

    select
        union_promotions.id as promotion_id
        ,union_promotions.promotion_source
        ,union_promotions.dbt_scd_id as promotion_key
        ,{{ clean_strings('union_promotions.promotion_type') }} as promotion_type
        ,promotion_code.promo_code as promo_code
        ,union_promotions.always_available as is_always_available
        ,union_promotions.must_be_assigned_to_user
        ,union_promotions.must_be_assigned_to_order
        ,union_promotions.claimable_window_in_days
        ,union_promotions.must_be_claimed
        ,union_promotions.must_be_applied_by_user
        ,union_promotions.starts_at as starts_at_utc
        ,union_promotions.ends_at as ends_at_utc
        ,union_promotions.created_at as created_at_utc
        ,union_promotions.updated_at as updated_at_utc
        ,union_promotions.dbt_valid_to
        ,union_promotions.dbt_valid_from
        ,case
            when union_promotions.dbt_valid_from = first_value(union_promotions.dbt_valid_from) over(partition by union_promotions.id order by union_promotions.dbt_valid_from) then '1970-01-01'
            else union_promotions.dbt_valid_from
        end as adjusted_dbt_valid_from
        ,coalesce(union_promotions.dbt_valid_to,'2999-01-01') as adjusted_dbt_valid_to
        ,promotion_key_value
    from union_promotions
        left join promotion_code on union_promotions.id = promotion_code.promotion_id
            and union_promotions.promotion_source = promotion_code.promotion_source
)

select * from renamed
