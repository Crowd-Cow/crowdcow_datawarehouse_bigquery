with

fc as ( select * from {{ ref('stg_cc__fcs') }} )

select * from fc
