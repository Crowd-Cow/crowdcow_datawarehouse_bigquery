with source as ( select * from {{ source('s3', 'fc_labor_hours') }} )

,renamed AS (
    SELECT
        PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(__filename, '[0-9]{8}')) AS labor_fiscal_month,
        --_modified AS modified_at_utc,
        {{ clean_strings('tasks_by_departmental_function_and_sub_department') }} AS task_description,
        gross_pay,
        base_pay_hours,
        coalesce(overtime_left_parenthesis_1_full_stop_5x_base_right_parenthesis____hours,cast(overtime_left_parenthesis_1_full_stop_5x_base_right_parenthesis__hours as string) ) AS overtime_hours,
        total_hours_worked,
        pto_hours_hours AS pto_hours,
        holiday_hours_hours AS holiday_hours,
        employee_count
    FROM source
)

,clean_values AS (
    SELECT
        {{ dbt_utils.surrogate_key(['labor_fiscal_month', 'task_description']) }} AS fc_labor_hours_id,
        labor_fiscal_month,
        task_description,
        REGEXP_REPLACE(cast(gross_pay as string), '[$,]', '') AS gross_pay,
        REGEXP_REPLACE(cast(base_pay_hours as string), '[$,]', '') AS base_pay_hours,
        REGEXP_REPLACE(cast(overtime_hours as string), '[$,]', '') AS overtime_hours,
        REGEXP_REPLACE(cast(total_hours_worked as string), '[$,]', '') AS total_hours_worked,
        REGEXP_REPLACE(cast(pto_hours as string), '[$,]', '') AS pto_hours,
        REGEXP_REPLACE(cast(holiday_hours as string), '[$,]', '') AS holiday_hours,
        REGEXP_REPLACE(cast(employee_count as string), '[$,]', '') AS employee_count
    FROM renamed
)

select * from clean_values
