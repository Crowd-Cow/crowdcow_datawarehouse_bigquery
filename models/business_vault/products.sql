with

product as ( select * from {{ ref('stg_cc__products') }} )

select * from product
