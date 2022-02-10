with

lot as ( select * from {{ ref('stg_cc__lots') }} )

select * from lot
