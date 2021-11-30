with source as (

    select * from {{ ref('promotions_ss') }} where not _fivetran_deleted

),

renamed as (

    select
        id as promotion_id
        , dbt_scd_id as promotion_key
        ,{{ clean_strings('promotion_type') }} as promotion_type
        ,always_available as promotion_is_always_available
        ,must_be_assigned_to_user as promotion_must_be_assigned_to_user
        ,must_be_assigned_to_order as promotion_must_be_assigned_to_order
        ,created_at as created_at_utc
        ,updated_at as updated_at_utc
        ,dbt_valid_to
        ,dbt_valid_from
        ,case
            when dbt_valid_from = first_value(dbt_valid_from) over(partition by id order by dbt_valid_from) then '1970-01-01'
            else dbt_valid_from
        end as adjusted_dbt_valid_from
        ,coalesce(dbt_valid_to,'2999-01-01') as adjusted_dbt_valid_to

    from source

)

select * from renamed
