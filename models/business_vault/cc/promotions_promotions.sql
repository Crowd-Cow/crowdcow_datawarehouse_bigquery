with

promotion as ( select * from {{ ref('stg_cc__promotions_promotions') }} )



select * from promotion
