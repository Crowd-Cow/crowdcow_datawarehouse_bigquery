with

inventory as ( select * from {{ ref('inventory_snapshot') }} )
,sku as ( select * from {{ ref('skus') }} )
,fc as ( select * from {{ ref('fcs') }} )

select
    inventory.snapshot_date
    ,sku.cut_id
    ,sku.cut_name
    ,fc.fc_id
    ,fc.fc_name
    ,ifnull(sku.category,'NONE') as category
    ,ifnull(sku.sub_category,'NONE') as sub_category
    ,iff(inventory.quantity < 0,0,inventory.quantity) as quantity
from inventory
    left join sku on inventory.sku_key = sku.sku_key
    left join fc on inventory.fc_key = fc.fc_key
where snapshot_date <= sysdate()::date - 1
