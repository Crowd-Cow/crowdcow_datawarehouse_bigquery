with

fc_labor_hour as ( select * from {{ ref('stg_s3__fc_labor_hours') }} )

,parse_tasks as (
    select
        *,
        CAST(SPLIT(task_description, ',')[SAFE_OFFSET(1)] AS STRING) AS task_description_short,
        REGEXP_EXTRACT(task_description, 'FC [A-Za-z]* ?[A-Za-z]*') AS fc_name
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

select * from categorize_tasks
