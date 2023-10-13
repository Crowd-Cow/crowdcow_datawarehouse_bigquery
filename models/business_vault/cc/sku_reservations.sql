with

sku_reservations as ( select * from {{ ref('stg_cc__sku_reservations') }} )

select * from sku_reservations