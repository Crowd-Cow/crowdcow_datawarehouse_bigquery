with

fiscal_calendar as ( select * from {{ source('cc', 'static_fiscal_calendar') }} )

select * from fiscal_calendar
