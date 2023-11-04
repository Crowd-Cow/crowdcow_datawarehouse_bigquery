with

sku_reservations as ( select * from {{ ref('stg_cc__sku_reservations') }} where dbt_valid_to is null )

select * from sku_reservations