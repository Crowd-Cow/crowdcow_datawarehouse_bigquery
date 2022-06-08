with source as ( select * from {{ source('s3', 'fc_labor_hours') }} )

,renamed as (
    select
        to_date(regexp_substr(_file,'[0-9]{8}'),'yyyymmdd') as labor_month
        ,_modified as modified_at_utc
        ,{{ clean_strings('tasks_by_departmental_function_and_sub_department') }} as task_description
        ,gross_pay
        ,base_pay_hours
        ,overtime_1_5_x_base_hours as overtime_hours
        ,total_hours_worked
        ,pto_hours_hours as pto_hours
        ,holiday_hours_hours as holiday_hours
        ,employee_count
    from source
)

,clean_values as (
    select
        {{ dbt_utils.surrogate_key(['labor_month', 'task_description']) }} as fc_labor_hours_id
        ,labor_month
        ,task_description
        ,regexp_replace(gross_pay,'[$,]*') as gross_pay
        ,regexp_replace(base_pay_hours,'[$,]*') as base_pay_hours
        ,regexp_replace(overtime_hours,'[$,]*') as overtime_hours
        ,regexp_replace(total_hours_worked,'[$,]*') as total_hours_worked
        ,regexp_replace(pto_hours,'[$,]*') as pto_hours
        ,regexp_replace(holiday_hours,'[$,]*') as holiday_hours
        ,regexp_replace(employee_count,'[$,]*') as employee_count
    from renamed
)

select * from clean_values
