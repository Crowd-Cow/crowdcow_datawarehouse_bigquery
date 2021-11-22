{{
    {{
      config(
        materialized = 'table',
        )
    }}
}}
-- year ends in January = 1
-- week start on Sunday = 0 

{{ fiscal_year_dates(1,0) }}
retail_periods as ( 

    select 
        calendar_date, 
        fiscal_year_number as retail_year_number, 
        week_start_date, 
        week_end_date, 
        fiscal_week_of_year as retail_week_of_year,
        fiscal_week_of_year-1 as week_num,
        -- Count the weeks in a 13 week period and separate the 4-5-4 week sequences
        mod(week_num::float, 13) as w13_number, 
        -- Chop weeks into 13 weeks merch quarters
        least(trunc(week_num/13),3) as quarter_number, 
        case 
            -- we move week 53 into the 3rd period of the quarter
            when fiscal_week_of_year = 53 then 3
            when w13_number between 0 and 3 then 1
            when w13_number between 4 and 7 then 2
            when w13_number between 8 and 12 then 3
        end as period_of_quarter, 
        (quarter_number*3) + period_of_quarter as retail_period_number
    from fiscal_year_dates

)
select 
    calendar_date, 
    retail_year_number,
    week_start_date, 
    week_end_date, 
    retail_week_of_year,
    dense_rank() over (
        partition by retail_year_number, retail_period_number
        order by retail_week_of_year
    ) as retail_week_of_period,
    retail_period_number, 
    quarter_number+1 as retail_quarter_number,
    period_of_quarter as retail_period_of_quarter
from retail_periods
order by 1,2