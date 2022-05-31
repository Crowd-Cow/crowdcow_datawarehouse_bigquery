with

sku as ( select * from {{ ref('skus') }} )

select *
from sku
--where is_cargill