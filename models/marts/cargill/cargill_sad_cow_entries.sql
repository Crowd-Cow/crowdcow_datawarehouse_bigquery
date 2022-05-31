with

sad_cow as ( select * from {{ ref('sad_cow_entries') }} )

select * from sad_cow
