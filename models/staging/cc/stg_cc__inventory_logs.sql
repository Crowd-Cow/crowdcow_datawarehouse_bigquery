with 

source as ( select * from {{ source('cc', 'inventory_logs') }} where not _fivetran_deleted )

,renamed as (
    select
        id as inventory_log_id
        ,sku_vendor_id
        ,{{ clean_strings('reason') }} as reason
        ,sku_id
        ,updated_at as updated_at_utc
        ,sku_box_id
        ,fc_id
        ,user_id
        ,created_at as created_at_utc
        ,sad_cow_bin_entry_id
        ,inventory_owner_id
        ,quantity as sku_quantity
        ,order_id
    from source
)

select * from renamed
