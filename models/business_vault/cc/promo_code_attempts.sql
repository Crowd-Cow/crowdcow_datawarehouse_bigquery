with

attemps as ( select * from {{ ref('stg_cc__promo_code_attempts') }} )

select * from attemps