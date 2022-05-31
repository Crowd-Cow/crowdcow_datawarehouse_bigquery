with

order_item_detail as ( select * from {{ ref('order_item_details') }} )
--,sku as ( select * from {{ ref('skus') }} )

select order_item_detail.*
from order_item_detail
    --left join sku on order_item_detail.sku_key = sku.sku_key
--where sku.is_cargill
