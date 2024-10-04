with 

source as ( select * from {{ source('cc', 'sku_plan_entries') }} )

,renamed as (
    select
        id as sku_plan_entry_id
        ,price_per_pound
        --,weight as sku_weight
        ,sku_plan_id
        ,cut_id
        ,created_at as created_at_utc
        ,updated_at as updated_at_utc
        ,receivable as is_receivable
    from source

)

select * from renamed
