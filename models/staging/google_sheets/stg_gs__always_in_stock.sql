with

source as ( select * from {{ source('google_sheets', 'always_in_stock_by_sku_name') }} )

,renamed as (
    select
        {{ dbt_utils.surrogate_key(['category','sub_category','cut_name','sku_name']) }} as ais_id
        ,category
        ,sub_category
        ,cut_name
        ,sku_name
        ,case when always_in_stock = 'Yes' then true else false end as is_always_in_stock
        ,case when active_item_sold = 'Yes' then true else false end as is_active_item_sold
    from source
)

select * from renamed
