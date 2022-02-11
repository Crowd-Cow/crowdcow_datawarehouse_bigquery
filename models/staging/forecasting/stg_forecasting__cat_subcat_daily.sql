with 

source as ( select * from {{ source('forecast', 'fc_cat_subcat_cut_daily_forecasts') }} )

,clean_up_id as (
    select
        case
            when item_id = 'null' then null
            else item_id
        end as item_id
        ,date
        ,p10
        ,p50
        ,p75
        ,p90
    from source
)

,renamed as (

    select
        {{ dbt_utils.surrogate_key(['item_id','date']) }} as forecast_id
        ,item_id
        ,split_part(item_id,'--',1)::int as fc_id
        ,{{ clean_strings("split_part(item_id,'--',2)") }} as category

        ,case
            when split_part(item_id,'--',3) = 'na' then null
            else {{ clean_strings("split_part(item_id,'--',3)") }}
         end as sub_category

        ,split_part(item_id,'--',4)::int as cut_id
        ,date as forecast_date
        ,p10
        ,p50
        ,p75
        ,p90

    from clean_up_id
)

select * from renamed

