with

sku as ( select * from {{ ref('skus') }} where not is_rastellis )

select *
from sku
--where is_cargill