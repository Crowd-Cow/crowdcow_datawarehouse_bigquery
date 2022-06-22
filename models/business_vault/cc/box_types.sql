with

box_type as ( select * from {{ ref('stg_cc__box_types') }} )

select * from box_type
