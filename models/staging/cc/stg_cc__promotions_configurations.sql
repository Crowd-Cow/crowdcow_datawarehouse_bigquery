with

source as ( select * from {{ source('cc', 'promotions_configurations') }} where __deleted is null )

,renamed as (
    select
        id as promotion_configuration_id
        ,{{ clean_strings('key') }} as promotion_configuration_key
        ,created_at as created_at_utc
        ,{{ clean_strings('value') }} as promotion_configuration_value
        ,{{ clean_strings('configurable_type') }} as configurable_type
        ,configurable_id
        ,updated_at as updated_at_utc
    from source
)

select * from renamed
