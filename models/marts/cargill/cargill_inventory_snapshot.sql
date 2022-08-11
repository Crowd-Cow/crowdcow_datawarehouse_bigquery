with

inventory as ( select * from {{ ref('inventory_snapshot') }} where not is_rastellis )
--,sku as ( select * from {{ ref('skus') }} )

select inventory.*
from inventory
    --left join sku on inventory.sku_key = sku.sku_key
--where sku.is_cargill
