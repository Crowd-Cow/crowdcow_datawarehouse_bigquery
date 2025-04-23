with references as ( select * from {{ ref('stg_gs__events_references') }} )

select * from references 