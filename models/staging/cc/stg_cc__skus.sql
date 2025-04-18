with 

source as ( select * from {{ ref('skus_ss') }} where _fivetran_deleted is null or _fivetran_deleted = false )
,raw_data as ( select * from {{ source('cc', 'skus') }} where __deleted is null  )

--The sku_snapshot CTE has been hardcoded to correct specific discrepancies identified within the snapshot table.
,sku_snapshot as (
    select source.*
    ,case 
        when coalesce(source.replenishment_code,'') != coalesce(raw_data.replenishment_code,'') and source.dbt_valid_to is null 
            then raw_data.replenishment_code else source.replenishment_code end as adjusted_replenishment_code
    ,case when raw_data.id is null and source.dbt_valid_to is null then true else false end as deleted
    from source
    left join raw_mysql.skus as raw_data on raw_data.id = source.id
)

,renamed as (

    select
        cast(id as int64) as sku_id
        ,dbt_scd_id as sku_key
        --,non_member_promotion_start_at as non_member_promotion_start_at_utc
        ,{{ cents_to_usd('average_cost_in_cents') }} as owned_sku_cost_usd
        ,promotion_start_at as promotion_start_at_utc
        ,{{ clean_strings('vendor_funded_discount_name') }} as vendor_funded_discount_name
        --,sku_code
        ,{{ cents_to_usd('vendor_funded_discount_cents') }} as vendor_funded_discount_usd
        --,non_member_promotion_discount
        --,member_only_promotion_discount
        --,member_only_promotion_start_at as member_only_promotion_start_at_utc
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
        --,member_only_promotion_end_at as member_only_promotion_end_at_utc
        ,sku_plan_entry_id
        ,reservation_window_days
        --,non_member_promotion_end_at as non_member_promotion_end_at_utc
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
        ,{{ clean_strings('adjusted_replenishment_code') }} as replenishment_code
        ,{{ clean_strings('vendor_product_code') }} as vendor_product_code
        ,vendor_case_pack_quantity as vendor_case_pack_quantity
        ,dbt_valid_to
        ,dbt_valid_from
        
        ,case
            when dbt_valid_from = first_value(dbt_valid_from) over(partition by id order by dbt_valid_from) then '1970-01-01'
            else dbt_valid_from
         end as adjusted_dbt_valid_from

        ,coalesce(dbt_valid_to,'2999-01-01') as adjusted_dbt_valid_to

    from sku_snapshot
    where not deleted

)

select * from renamed
