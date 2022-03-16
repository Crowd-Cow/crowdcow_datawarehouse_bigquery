with

sku as ( select * from {{ ref('skus') }} )

select
    cut_id
    ,cut_name
    ,category
    ,sub_category
from sku
where dbt_valid_to is null
