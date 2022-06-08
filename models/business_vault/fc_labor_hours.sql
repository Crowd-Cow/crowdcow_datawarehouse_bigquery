with

fc_labor_hour as ( select * from {{ ref('stg_s3__fc_labor_hours') }} )

,parse_tasks as (
    select
        *
        ,split(task_description,',')[1]::text as task_description_short
        ,regexp_substr(task_description,'FC [A-Za-z]* {0,}[A-Za-z]{0,}') as fc_name
    from fc_labor_hour
)

,categorize_tasks as (
    select
        *

        ,case
            when task_description_short like '%BOX MAKING%' then 'BOX MAKING'
            when task_description_short like '%PICKING%' then 'PICKING'
            when task_description_short like '%PACKING%' then 'PACKING'
            when task_description_short is null then 'SUMMARY'
            else 'ALL OTHER'
        end as hours_category
        
    from parse_tasks
)

select * from parse_tasks
