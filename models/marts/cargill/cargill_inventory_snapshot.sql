with

inventory as ( select * from {{ ref('inventory_snapshot') }} )
--,sku as ( select * from {{ ref('skus') }} )

select inventory.*
from inventory
    --left join sku on inventory.sku_key = sku.sku_key
--where sku.is_cargill
