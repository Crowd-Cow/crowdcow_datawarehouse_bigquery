with

cut as ( select * from {{ ref('stg_cc__cuts') }} )

select * from cut
