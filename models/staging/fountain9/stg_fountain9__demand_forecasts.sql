with

source as ( select * from {{ ref('demand_forecast_ss') }} where dbt_valid_to is null )

,renamed as (
    select
        id as forecast_id
        ,date as forecast_date
        ,cast(fc_id as INT64) as fc_id
        ,{{ clean_strings('fc_name') }} as fc_name
        ,{{ clean_strings('category') }} as category
        ,{{ clean_strings('sub_category') }} as sub_category
        ,cast(cut_id as INT64) as cut_id
        ,{{ clean_strings('cut_name') }} as cut_name
        ,{{ clean_strings('inventory_classification') }} as inventory_classification
        ,predicted_quantity as forecasted_sales
    from source
)

,clean_sub_category as (
    select
        forecast_id 
        ,forecast_date
        ,fc_id
        ,fc_name
        ,category

        /*** String cleaning is required since F9 changes the values of the data we send to them ***/
        /*** this was causing a misatch when trying to join on sub_category since we use this as a part of our "key" ***/
        ,case
            when sub_category = 'ORGANIC 100% GRASS FED' then 'ORGANIC, 100% GRASS FED'
            when sub_category = 'NOT SPECIFIED' then null
            else sub_category
         end as sub_category

        ,cut_id
        ,cut_name
        ,inventory_classification
        ,forecasted_sales
    from renamed
)

select
     {{ dbt_utils.surrogate_key(['forecast_date','fc_id','category','sub_category','cut_id']) }} as forecast_id
    ,forecast_date
    ,fc_id
    ,fc_name
    ,category
    ,sub_category
    ,cut_id
    ,cut_name
    ,inventory_classification
    ,sum(forecasted_sales) as forecasted_sales
from clean_sub_category
group by 1,2,3,4,5,6,7,8,9
