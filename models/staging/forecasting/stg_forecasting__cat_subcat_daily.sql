with source as (

    select * from {{ source('forecast', 'fc_cat_subcat_cut_daily_forecasts') }}

),

renamed as (

    select
        {{ dbt_utils.surrogate_key(['item_id','date']) }} as forecast_id
        ,item_id
        ,split_part(item_id,'--',1)::int as fc_id
        ,{{ clean_strings("split_part(item_id,'--',2)") }} as category
        ,{{ clean_strings("split_part(item_id,'--',3)") }} as sub_category
        ,split_part(item_id,'--',4)::int as cut_id
        ,date as forecast_date
        ,p10
        ,p50
        ,p75
        ,p90

    from source
    where item_id <> 'null'

)

select * from renamed

