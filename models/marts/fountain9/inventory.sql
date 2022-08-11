with

inventory as ( select * from {{ ref('inventory_snapshot') }} where not is_rastellis )
,sku as ( select * from {{ ref('skus') }} )
,fc as ( select * from {{ ref('fcs') }} )

,cleanup_cateogries_quantities as (
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
)

select
    snapshot_date
    ,cut_id
    ,cut_name
    ,fc_id
    ,fc_name
    ,category
    ,sub_category
    ,sum(quantity) as quantity
from cleanup_cateogries_quantities
group by 1,2,3,4,5,6,7
