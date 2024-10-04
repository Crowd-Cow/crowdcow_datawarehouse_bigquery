with

promotion as ( select * from {{ ref('stg_cc__promotions_promo_codes') }} )



select * from promotion
