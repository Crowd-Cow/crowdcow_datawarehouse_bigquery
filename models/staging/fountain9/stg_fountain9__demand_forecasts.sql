with

source as ( select * from {{ ref('demand_forecast_ss') }} where dbt_valid_to is null )

,renamed as (
    select
        id as forecast_id
        ,date as forecast_date
        ,fc_id::int as fc_id
        ,{{ clean_strings('fc_name') }} as fc_name
        ,{{ clean_strings('category') }} as category
        ,{{ clean_strings('sub_category') }} as sub_category
        ,cut_id::int as cut_id
        ,{{ clean_strings('cut_name') }} as cut_name
        ,{{ clean_strings('inventory_classification') }} as inventory_classification
        ,predicted_quantity as forecasted_sales
    from source
)

select * from renamed
