with

source as ( select * from {{ source('google_sheets', 'always_in_stock_by_sku_name') }} )

,renamed as (
    select
        {{ dbt_utils.surrogate_key(['category','sub_category','cut_name','sku_name']) }} as ais_id
        ,category
        ,sub_category
        ,cut_name
        ,sku_name
        ,always_in_stock::boolean as is_always_in_stock
        ,active_item_sold::boolean as is_active_item_sold
    from source
)

select * from renamed
