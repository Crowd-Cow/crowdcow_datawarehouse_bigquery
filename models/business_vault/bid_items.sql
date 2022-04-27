with

bid_item as ( select * from {{ ref('stg_cc__bid_items') }} )

select * from bid_item
