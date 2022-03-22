with

sku as ( select * from {{ ref('skus') }} )

select
    cut_id
    ,cut_name
    ,category
    ,sub_category
    ,sku_id
    ,sku_name
    
    ,case
        when is_always_in_stock then 'AIS'
        else 'OTHER'
     end as inventory_classification
    
    ,sku_vendor_name as brand
    ,'UNITS' as uom
from sku
where dbt_valid_to is null
    and cut_id is not null
    and category is not null
