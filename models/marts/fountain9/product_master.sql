with

sku as ( select * from {{ ref('skus') }} )

select
    cut_id
    ,cut_name
    ,category
    ,sub_category
    ,sku_id
    ,sku_name
    ,is_always_in_stock
    ,sku_vendor_name as brand
    ,'units' as uom
from sku
where dbt_valid_to is null
