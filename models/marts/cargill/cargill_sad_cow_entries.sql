with

sad_cow as ( select * from {{ ref('sad_cow_entries') }} where not is_rastellis or is_rastellis is null )

select * from sad_cow
