with 

day_spine as (
    
    {{
      dbt_utils.date_spine(
          datepart = 'day',
          start_date = "'2018-01-01'::date",
          end_date = "date(sysdate())"
      )
    }}

),

date_parts as (

    select
        date_day
        ,date_trunc('week',date_day) as date_week
        ,date_trunc('month',date_day) as date_month
        ,date_trunc('quarter',date_day) as date_calendar_quarter
        ,date_trunc('year',date_day) as date_year
        ,date_part(week,date_day) as week_of_year
        ,date_part(quarter,date_day) as calendar_quarter_of_year
        ,date_part(dow,date_day) as day_of_week
        ,dayname(date_day) as day_name
        ,case
            when date_part(dow,date_day) in (0,6) then true
            else false
        end as is_weekend
from day_spine

)

select * from date_parts
