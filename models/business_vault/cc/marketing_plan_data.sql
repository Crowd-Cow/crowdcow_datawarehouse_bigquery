with

marketing_plan_data as (select * from {{ ref('stg_gs__marketing_plan_data') }})

select * from marketing_plan_data