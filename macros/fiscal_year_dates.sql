{% macro fiscal_year_dates(year_end_month, week_start_day=0, shift_year=1) %}
-- this gets all the dates within a fiscal year 
-- determined by the given year-end-month
-- ending on the saturday closest to that month's end date
with year_month_end as (

    select
        -- This, while slightly lazy, accounts for the fact
        -- that most fiscal years end in a month following the calendar year
       d.year_number-{{ shift_year }} as fiscal_year_number,
       d.month_end_date
    from
        {{ ref('dates') }} d
    where
        d.month_of_year = {{ year_end_month }}
    group by 1,2

),
weeks as (

    select
        d.calendar_date as week_start_date,
        dateadd('d', 6, d.calendar_date) as week_end_date
    from
        {{ ref('dates') }} d
    where 
        date_part('dow', d.calendar_date) = {{ week_start_day }}

),
-- get all the weeks that start in the month the year ends
year_week_ends as (

    select
        d.year_number-{{ shift_year }} as fiscal_year_number,
        d.week_end_date
    from
        weeks d
    where
        date_part('month', d.week_start_date) = {{ year_end_month }}
    group by 1,2

),
-- then calculate which Saturday is closest to month end
weeks_at_month_end as (

    select
        d.fiscal_year_number,
        d.week_end_date,
        m.month_end_date,
        rank() over
            (partition by d.fiscal_year_number
                order by
                abs(datediff('d', d.week_end_date, m.month_end_date))

            ) as closest_to_month_end
    from
        year_week_ends d
        join
        year_month_end m on d.fiscal_year_number = m.fiscal_year_number

),
fiscal_year_range as  (

    select
        fiscal_year_number,
        dateadd('day', 1, 
            lag(week_end_date) over(order by week_end_date)
        ) as fiscal_year_start_date,
        week_end_date as fiscal_year_end_date
    from
        weeks_at_month_end
    where closest_to_month_end = 1

),
fiscal_year_dates as (

    select
        d.calendar_date,
        m.fiscal_year_number,
        m.fiscal_year_start_date,
        m.fiscal_year_end_date,
        w.week_start_date,
        w.week_end_date,
        -- we reset the weeks of the year starting with the merch year start date
        dense_rank() 
            over(
                partition by m.fiscal_year_number 
                order by w.week_start_date
                ) as fiscal_week_of_year 
    from
        {{ ref('dates') }} d
        join
        fiscal_year_range m on d.calendar_date between m.fiscal_year_start_date and m.fiscal_year_end_date
        join
        weeks w on d.calendar_date between w.week_start_date and w.week_end_date

),
{% endmacro %}