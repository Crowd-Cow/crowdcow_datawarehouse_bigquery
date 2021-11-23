with 

day_spine as (
    /* Using 2015-01-04 as the first fiscal year start date because that is the min(fiscal_start_date) in the current model - crowdcow_dbt.dim_fiscal_calendar*/
    {{
      dbt_utils.date_spine(
          datepart = 'day',
          start_date = "'2015-01-04'::date",
          end_date = "dateadd(week, 53, current_date)"
      )
    }}

),

date_parts as (
/* Do we want week start/end and month start/end */ 
    select
        date_day as calendar_date
        ,date_trunc('week',date_day) as date_week
        ,date_trunc(week,date_day+1)-1 as date_week_sun
        ,date_trunc('month',date_day) as date_month
        ,date_part('month',date_day)::int as month_of_year
        ,{{ dbt_utils.last_day('date_day', 'month') }} as month_end_date
        ,date_trunc('quarter',date_day) as date_calendar_quarter
        ,date_trunc('year',date_day) as date_year
        ,date_part(week,date_day) as week_of_year
        ,date_part(quarter,date_day) as calendar_quarter_of_year
        ,date_part(dow,date_day) as day_of_week
        ,dayname(date_day) as day_name
        ,date_part('year', date_day)::int as year_number
        ,case
            when date_part(dow,date_day) in (0,6) then true
            else false
        end as is_weekend
        ,case when mod((date_part('year',date_day) - 2015),4) = 0 then true
            else false
            end as is_53_wk_year
from day_spine

)

select * from date_parts
