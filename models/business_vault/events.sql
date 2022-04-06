with

events as ( select * from {{ ref('int_events') }} )

select * from events