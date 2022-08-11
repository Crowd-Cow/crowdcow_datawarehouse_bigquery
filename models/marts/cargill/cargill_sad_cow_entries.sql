with

sad_cow as ( select * from {{ ref('sad_cow_entries') }} where not is_rastellis )

select * from sad_cow
