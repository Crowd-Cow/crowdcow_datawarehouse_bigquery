with source as (

    select * from {{ source('cc', 'skus') }} where not _fivetran_deleted

),

renamed as (

    select
        id as sku_id
        ,non_member_promotion_start_at as non_member_promotion_start_at_utc
        ,{{ cents_to_usd('average_cost_in_cents') }} as average_cost_usd
        ,promotion_start_at as promotion_start_at_utc
        ,{{ clean_strings('vendor_funded_discount_name') }} as vendor_funded_discount_name
        ,sku_code
        ,{{ cents_to_usd('vendor_funded_discount_cents') }} as vendor_funded_discount_usd
        ,non_member_promotion_discount
        ,member_only_promotion_discount
        ,member_only_promotion_start_at as member_only_promotion_start_at_utc
        ,updated_at as updated_at_utc
        ,{{ cents_to_usd('platform_fee_in_cents') }} as platform_fee_usd
        ,{{ clean_strings('name') }} as sku_name
        ,{{ cents_to_usd('fulfillment_fee_in_cents') }} as fulfillment_fee_usd
        ,{{ cents_to_usd('price_in_cents') }} as sku_price_usd
        ,{{ cents_to_usd('marketplace_cost_in_cents') }} as marketplace_cost_usd
        ,created_at as created_at_utc
        ,sku_vendor_id
        ,average_box_quantity
        ,vendor_funded_discount_start_at as vendor_funded_discount_start_at_utc
        ,vendor_funded_discount_end_at as vendor_funded_discount_end_at_utc
        ,barcode as sku_barcode
        ,{{ cents_to_usd('payment_processing_fee_in_cents') }} as payment_processing_fee_usd
        ,cut_id
        ,{{ convert_percent('vendor_funded_discount_percent') }} as vendor_funded_discount_percent
        ,active_at as active_at_utc
        ,promotion_end_at as promotion_end_at_utc
        ,weight as sku_weight
        ,{{ cents_to_usd('standard_price_in_cents') }} as standard_price_usd
        ,{{ cents_to_usd('promotional_price_in_cents') }} as promotional_price_usd
        ,member_only_promotion_end_at as member_only_promotion_end_at_utc
        ,sku_plan_entry_id
        ,reservation_window_days
        ,non_member_promotion_end_at as non_member_promotion_end_at_utc
        ,bulk_receivable as is_bulk_receivable
        ,is_presellable
        ,virtual_inventory as is_virtual_inventory
        ,member_discount_start_at as member_discount_start_at_utc
        ,general_discount_start_at as general_discount_start_at_utc
        ,general_discount_end_at as general_discount_end_at_utc
        ,member_discount_end_at as member_discount_end_at_utc
        ,{{ convert_percent('member_discount_percent') }} as member_discount_percent
        ,{{ convert_percent('general_discount_percent') }} as general_discount_percent
        ,non_member_discount_end_at as non_member_discount_end_at_utc
        ,{{ convert_percent('non_member_discount_percent') }} as non_member_discount_percent
        ,non_member_discount_start_at as non_member_discount_start_at_utc
        ,{{ convert_percent('partial_member_discount_percent') }} as partial_member_discount_percent

    from source

)

select * from renamed

