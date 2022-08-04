with

partner as ( select * from {{ ref('stg_cc__partners') }} )

select * from partner
