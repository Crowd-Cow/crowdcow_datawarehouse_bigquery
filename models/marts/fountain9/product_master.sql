with

sku as ( select * from {{ ref('skus') }} )

select
    cut_id
    ,cut_name
    ,category
    ,sub_category
    ,sku_vendor_name as brand
    ,'units' as uom
from sku
where dbt_valid_to is null
