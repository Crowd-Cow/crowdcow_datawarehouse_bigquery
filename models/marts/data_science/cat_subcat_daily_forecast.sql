with

forecast as ( select * from {{ ref('stg_forecasting__cat_subcat_daily') }} )

select * from forecast
