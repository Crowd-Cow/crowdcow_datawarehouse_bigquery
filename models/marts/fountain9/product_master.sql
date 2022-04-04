with

sku as ( select * from {{ ref('skus') }} )

select distinct
    cut_id
    ,cut_name
    ,category
    ,sub_category

    ,case
        when is_always_in_stock then 'AIS'
        else 'OTHER'
     end as inventory_classification
    
    ,sku_vendor_name as brand
    ,'1 YEAR' as shelf_life
    ,'UNITS' as uom
    ,round(avg(sku_weight),2) as avg_product_weight
from sku
where dbt_valid_to is null
    and cut_id is not null
    and category is not null
group by 1,2,3,4,5,6,7,8
