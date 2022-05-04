with

sku_lot as ( select * from {{ ref('stg_cc__sku_lots') }} )
lot as ( select * from {{ ref('stg_cc__lots') }} )