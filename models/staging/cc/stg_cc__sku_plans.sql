with 

source as ( select * from {{ source('cc', 'sku_plans') }} where not _fivetran_deleted )

,renamed as (
    select
        id as sku_plan_id
        ,updated_at as updated_at_utc
        ,{{ clean_strings('sku_plan_type') }} as sku_plan_type
        ,{{ clean_strings('name') }} as sku_plan_name
        ,created_at as created_at_utc
    from source
)

select * from renamed
