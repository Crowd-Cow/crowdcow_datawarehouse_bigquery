with

contact as ( select * from {{ ref('stg_pb__contacts') }} )

select * from contact
