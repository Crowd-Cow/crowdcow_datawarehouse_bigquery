with

order_packed as ( select * from {{ ref('stg_cc__order_packed_skus') }} )

select * from order_packed