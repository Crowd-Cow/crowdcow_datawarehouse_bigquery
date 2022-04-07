with

sku as ( select * from {{ ref('skus') }} )
,cuts as ( select * from {{ ref('cuts') }} )
,moq as ( select * from {{ ref('stg_gs__inventory_moq') }} )

select distinct
    sku.cut_id
    ,sku.cut_name
    ,sku.category
    ,sku.sub_category

    ,case
        when sku.is_always_in_stock then 'AIS'
        when sku.inventory_classification is not null then sku.inventory_classification
        else 'OTHER'
     end as inventory_classification
    
    ,sku.sku_vendor_id as brand_id
    ,sku.sku_vendor_name as brand
    ,'1 YEAR' as shelf_life
    ,'UNITS' as uom
    ,moq.case_pack as batch_size
    ,moq.case_pack as moq
    ,round(avg(sku.sku_weight),2) as avg_product_weight
from sku
    inner join cuts on sku.cut_key = cuts.cut_key
        and cuts.plu is not null
        and cuts.dbt_valid_to is not null
    left join moq on sku.cut_id = moq.cut_id
        and sku.farm_id = moq.farm_id
where sku.dbt_valid_to is null
    and sku.cut_id is not null
    and sku.category is not null
group by 1,2,3,4,5,6,7,8,9,10
