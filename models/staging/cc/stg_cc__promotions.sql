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

    from source

)

select * from renamed
