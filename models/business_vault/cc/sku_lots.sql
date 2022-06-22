with

sku_lot as ( select * from {{ ref('stg_cc__sku_lots') }} )
,lot as ( select * from {{ ref('stg_cc__lots') }} )
,sku as ( select * from {{ ref('stg_cc__skus') }} )

,get_keys as (
    select
        sku_lot.*
        ,lot.lot_key
        ,sku.sku_key
    from sku_lot 
        left join lot on sku_lot.lot_id = lot.lot_id 
            and sku_lot.created_at_utc >= lot.adjusted_dbt_valid_from
            and sku_lot.created_at_utc < lot.adjusted_dbt_valid_to
        left join sku on sku_lot.sku_id = sku.sku_id
            and sku_lot.created_at_utc >= sku.adjusted_dbt_valid_from
            and sku_lot.created_at_utc < sku.adjusted_dbt_valid_to
)

select
    sku_lot_id
    ,sku_id
    ,sku_key
    ,lot_id
    ,lot_key
    ,sku_lot_cost_usd
    ,sku_lot_quantity
    ,created_at_utc
    ,updated_at_utc
from get_keys
