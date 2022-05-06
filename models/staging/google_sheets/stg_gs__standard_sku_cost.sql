with

source as ( select * from {{ source('google_sheets', 'standard_sku_cost') }} )

,renamed as (
    select
        sku_id
        ,cost as standard_sku_cost_usd
    from source
)

select * from renamed
