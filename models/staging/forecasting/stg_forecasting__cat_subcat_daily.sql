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

