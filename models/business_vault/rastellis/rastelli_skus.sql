with

sku as ( select * from {{ ref('stg_cc__skus') }} where sku_vendor_id = 280 )

select
    sku_id
    ,sku_key
    ,updated_at_utc
    ,sku_name
    ,created_at_utc
    ,sku_vendor_id
    ,sku_barcode
    ,sku_weight
    ,is_bulk_receivable
    ,is_virtual_inventory
    ,dbt_valid_to
    ,dbt_valid_from
    ,adjusted_dbt_valid_from
    ,adjusted_dbt_valid_to
from sku
