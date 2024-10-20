with

source as ( select * from {{ source('cc', 'promotions_promo_codes') }} where __deleted is null  )


SELECT * from source

