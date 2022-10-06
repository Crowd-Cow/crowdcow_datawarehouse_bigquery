with

sku as ( select * from {{ ref('skus') }} where not is_rastellis or is_rastellis is null )

select *
from sku
--where is_cargill