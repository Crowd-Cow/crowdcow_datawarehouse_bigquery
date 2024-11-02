with

q4_forecast as (select * from {{ ref('stg_gs__q4_forecast') }})

select * from q4_forecast