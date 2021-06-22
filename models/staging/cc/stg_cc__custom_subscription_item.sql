with source as (

    select * from {{ source('cc', 'custom_subscription_items') }}

),

renamed as (

    select
        id as custom_subscription_id
        ,{{ clean_strings('meat_preference') }} as meat_preference
        ,product_id
        ,updated_at as updated_at_utc
        ,product_variant_id
        ,subscription_id
        ,created_at
        ,quantity

    from source

)

select * from renamed

