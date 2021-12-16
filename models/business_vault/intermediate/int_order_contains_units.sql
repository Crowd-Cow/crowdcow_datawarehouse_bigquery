with

orders as ( select * from {{ ref('stg_cc__orders') }} )

, is_bundle as ( select * from {{ ref('stg_cc_bids') }})