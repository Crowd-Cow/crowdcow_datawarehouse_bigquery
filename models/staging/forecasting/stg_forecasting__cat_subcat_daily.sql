with 

source as ( select * from {{ ref('fc_cat_subcat_cut_daily_forecast_ss') }} where dbt_valid_to is null)

,clean_up_id as (
    select
        id as forecast_id
        ,case
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
        forecast_id
        ,item_id
        ,SAFE_CAST(split(item_id,'--')[OFFSET(0)] as INT64) as fc_id
        ,{{ clean_strings("split(item_id,'--')[OFFSET(1)]") }} as category

        ,case
            when split(item_id,'--')[OFFSET(2)] = 'na' then null
            else {{ clean_strings("split(item_id,'--')[OFFSET(2)]") }}
         end as sub_category

        ,SAFE_CAST(split(item_id,'--')[OFFSET(3)] as INT64) as cut_id
        ,date as forecast_date
        ,p10
        ,p50
        ,p75
        ,p90

    from clean_up_id
)

select * from renamed

