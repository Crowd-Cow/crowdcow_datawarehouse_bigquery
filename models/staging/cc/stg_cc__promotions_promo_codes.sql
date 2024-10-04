with

source as ( select * from {{ source('cc', 'promotions_promo_codes') }} )


SELECT * from source

