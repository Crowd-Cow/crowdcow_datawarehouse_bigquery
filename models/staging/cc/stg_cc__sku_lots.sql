with

source as ( select * from {{ source('cc', 'sku_lots') }}  )

,renamed as (
    select
        id as sku_lot_id
        ,updated_at as updated_at_utc
        ,{{ cents_to_usd('cost_in_cents') }} as sku_lot_cost_usd
        ,sku_id
        ,quantity as sku_lot_quantity
        ,lot_id
        ,created_at as created_at_utc
    from source
)

select * from renamed
