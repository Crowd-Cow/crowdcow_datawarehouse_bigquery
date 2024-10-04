with 

date_spine as ( select * from {{ ref('stg_reference__date_spine') }} )
,static_fiscal_calendar as ( select * from {{ ref('stg_reference__static_fiscal_calendar') }} )

,add_fiscal_periods as (
    select
        date_spine.*
        ,static_fiscal_calendar.fiscal_week as fiscal_week_num
        ,static_fiscal_calendar.fiscal_quarter
        ,static_fiscal_calendar.fiscal_year
        ,static_fiscal_calendar.fiscal_month as fiscal_month
    from date_spine
        left join static_fiscal_calendar on date_spine.calendar_date = cast(static_fiscal_calendar.calendar_date as date)
)


/**** We do not use a standard fiscal calendar and instead have opted for a custom created fiscal calendar *****/
/**** The rules for the custom calendar are arbitrary and not well defined making it impossible to automate ****/
/**** Instead a manually created spreadsheet with fiscal weeks have been uploaded to use as a reference ****/
/**** The commented code below represents an attempt to automate the custom fiscal calendar and the logic is not correct ****/

/*,retail_periods as ( 
    select
        *
        ,dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) as fiscal_week_num
        ,case
            when is_53_wk_year then
                case
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 1 and 5 then 1
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 6 and 9 then 2
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 10 and 14 then 3
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 15 and 18 then 4
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 19 and 22 then 5
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 23 and 27 then 6
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 28 and 31 then 7
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 32 and 35 then 8
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 36 and 40 then 9
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 41 and 44 then 10
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 45 and 48 then 11
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 49 and 53 then 12
                end  
            else
                case
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 1 and 4 then 1
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 5 and 8 then 2
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 9 and 13 then 3
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 14 and 17 then 4
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 18 and 21 then 5
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 22 and 26 then 6
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 27 and 30 then 7
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 31 and 34 then 8
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 35 and 39 then 9
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 40 and 43 then 10
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 44 and 47 then 11
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 48 and 52 then 12
                end
            end as fiscal_month 
        ,case
            when is_53_wk_year then
                case
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 1 and 14 then 1
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 15 and 27 then 2
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 28 and 40 then 3
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 41 and 53 then 4
                end
            else
                case
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 1 and 13 then 1
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 14 and 26 then 2
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 27 and 39 then 3
                    when dense_rank() over(partition by fiscal_year_start order by fiscal_year_start,calendar_date_week_sun) between 40 and 52 then 4
                end
        end as fiscal_quarter

        ,year(fiscal_year_start) as fiscal_year

    from {{ ref('stg_reference__date_spine') }}
)

select * from retail_periods*/

select * from add_fiscal_periods 