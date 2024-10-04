with 

source as ( select * from {{ source('cc', 'sad_cow_bin_entries') }}  )

,renamed as (
    select
        id as sad_cow_bin_entry_id
        ,created_at as created_at_utc
        ,{{ clean_strings ('reason') }} as sad_cow_reason
        ,sku_vendor_id
        ,{{ clean_strings('entry_type') }} as sad_cow_entry_type
        ,sku_id
        ,weight as sku_weight
        --, clean_strings('details') }} as sad_cow_details
        ,quantity as sku_quantity
        ,updated_at as updated_at_utc
        ,sad_cow_bin_id
        ,lot_id
        ,user_id
        ,sku_box_id
        ,cut_id
        ,{{ clean_strings('action_taken') }} as sad_cow_action_taken
        --, clean_strings('photo_urls') }} as photo_urls
    from source
)

select * from renamed
