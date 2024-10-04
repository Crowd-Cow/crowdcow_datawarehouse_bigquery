with 

day_spine as (
    /* Using 2015-01-04 as the first fiscal year start date because that is the min(fiscal_start_date) in the current model - crowdcow_dbt.dim_fiscal_calendar*/
    {{
      dbt_utils.date_spine(
          datepart = 'day',
          start_date = "cast('2015-01-04' as date)",
          end_date = "date_add(current_date(), interval 53 week)"
      )
    }}

)

,date_parts as (
SELECT
    date_day AS calendar_date,
    DATE_TRUNC(date_day, WEEK(MONDAY)) AS calendar_date_week,
    DATE_SUB(DATE_TRUNC(DATE_ADD(date_day, INTERVAL 1 DAY), WEEK(MONDAY)), INTERVAL 1 DAY) AS calendar_date_week_sun,
    DATE_TRUNC(date_day, MONTH) AS calendar_date_month,
    EXTRACT(MONTH FROM date_day) AS calendar_month_of_year,
    {{ dbt_utils.last_day('date_day', 'MONTH') }} AS calendar_month_end_date,
    DATE_TRUNC(date_day, QUARTER) AS calendar_date_quarter,
    DATE_TRUNC(date_day, YEAR) AS calendar_date_year,
    EXTRACT(ISOWEEK FROM date_day) AS calendar_week_of_year,
    EXTRACT(QUARTER FROM date_day) AS calendar_quarter_of_year,
    (EXTRACT(DAYOFWEEK FROM date_day) -1 ) AS day_of_week,
    FORMAT_DATE('%A', date_day) AS day_name,
    EXTRACT(YEAR FROM date_day) AS calendar_year_number,
    MIN(date_day) OVER(PARTITION BY EXTRACT(YEAR FROM DATE_SUB(DATE_TRUNC(DATE_ADD(date_day, INTERVAL 1 DAY), WEEK(MONDAY)), INTERVAL 1 DAY)) ORDER BY date_day) AS fiscal_year_start,

    MAX(
        CASE
            WHEN EXTRACT(YEAR FROM DATE_SUB(DATE_TRUNC(DATE_ADD(date_day, INTERVAL 1 DAY), WEEK(MONDAY)), INTERVAL 1 DAY)) = EXTRACT(YEAR FROM date_day)
                THEN DATE_SUB(DATE_TRUNC(DATE_ADD(date_day, INTERVAL 1 DAY), WEEK(MONDAY)), INTERVAL 1 DAY)
        END
    ) OVER(PARTITION BY EXTRACT(YEAR FROM date_day)) + INTERVAL 6 DAY AS fiscal_year_end,

    CASE
        WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE
        ELSE FALSE
    END AS is_weekend,

    CASE
        WHEN MOD(ABS(EXTRACT(YEAR FROM date_day) - 2017), 4) = 0 THEN TRUE
        ELSE FALSE
    END AS is_53_wk_year

FROM 
    day_spine

)

select * from date_parts
