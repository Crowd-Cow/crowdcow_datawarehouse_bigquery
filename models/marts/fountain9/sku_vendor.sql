with
inventory as ( select * from {{ ref('inventory_snapshot') }} where not is_rastellis or is_rastellis is null )
,sku as ( select * from {{ ref('skus') }} )
,fc as ( select * from {{ ref('fcs') }} )
,cut as ( select * from {{ ref('cuts') }} )

select distinct
    sku.cut_id
    ,sku.cut_name
    ,inventory.fc_id
    ,fc.fc_name
    ,sku.sku_vendor_id as brand_id
    ,sku.sku_vendor_name as brand
    ,1 as schedule
from inventory
    left join sku on inventory.sku_key = sku.sku_key
    left join fc on inventory.fc_key = fc.fc_key
    left join cut on sku.cut_key = cut.cut_key
where sku.cut_id is not null
    and cut.plu is not null
    and inventory.fc_id not in (8,10,14) --filter out Poseidon, Nationwide, and Valmeyer FCs
