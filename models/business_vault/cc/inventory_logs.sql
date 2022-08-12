with

inventory_log as ( select * from {{ ref('stg_cc__inventory_logs') }} )
,sad_cow_entry as ( select * from {{ ref('stg_cc__sad_cow_entries') }} )
,lot as ( select * from {{ ref('stg_cc__lots') }} where dbt_valid_to is null )
,fc as ( select * from {{ ref('stg_cc__fcs') }} )
,sku_vendor as ( select * from {{ ref('stg_cc__sku_vendors') }} )
,sku as ( select * from {{ ref('stg_cc__skus') }} )
,current_sku_box as ( select * from {{ ref('stg_cc__sku_boxes') }} where dbt_valid_to is null )

,get_inventory_lot as (
    select 
        inventory_log.inventory_log_id
        ,inventory_log.sku_vendor_id
        ,inventory_log.reason
        ,inventory_log.sku_id
        ,inventory_log.updated_at_utc
        ,lot.lot_number
        ,inventory_log.sku_box_id
        ,inventory_log.fc_id
        ,inventory_log.user_id
        ,inventory_log.created_at_utc
        ,inventory_log.sad_cow_bin_entry_id
        ,lot.owner_id as inventory_owner_id
        ,inventory_log.sku_quantity
        ,inventory_log.order_id
    from inventory_log
        left join current_sku_box on inventory_log.sku_box_id = current_sku_box.sku_box_id
        left join lot on current_sku_box.lot_id = lot.lot_id
    
)

,get_sad_cow_receiving_log as (
    select
        sad_cow_entry.sad_cow_bin_entry_id
        ,sad_cow_entry.sku_vendor_id
        ,sad_cow_entry.sku_id
        ,sad_cow_entry.updated_at_utc
        ,lot.lot_number
        ,sad_cow_entry.sku_box_id
        ,lot.fc_id
        ,sad_cow_entry.user_id
        ,sad_cow_entry.created_at_utc
        ,lot.owner_id
        ,sad_cow_entry.sku_quantity
    from sad_cow_entry
        left join lot on sad_cow_entry.lot_id = lot.lot_id
            and sad_cow_entry.created_at_utc >= lot.adjusted_dbt_valid_from
            and sad_cow_entry.created_at_utc < lot.adjusted_dbt_valid_to
    where sad_cow_entry.sad_cow_entry_type = 'RECEIVING'
)

,union_logs as (
    select
        inventory_log_id
        ,sku_vendor_id
        ,reason
        ,sku_id
        ,updated_at_utc
        ,lot_number
        ,sku_box_id
        ,fc_id
        ,user_id
        ,created_at_utc
        ,sad_cow_bin_entry_id
        ,inventory_owner_id
        ,sku_quantity
        ,order_id
    from get_inventory_lot
    
    union all
    
    select
        sad_cow_bin_entry_id as inventory_log_id
        ,sku_vendor_id
        ,'SAD_COW_RECEIVING' as reason
        ,sku_id
        ,updated_at_utc
        ,lot_number
        ,sku_box_id
        ,fc_id
        ,user_id
        ,created_at_utc
        ,sad_cow_bin_entry_id
        ,owner_id as inventory_owner_id
        ,-sku_quantity
        ,null::int as order_id
    from get_sad_cow_receiving_log

    union all
    
    select
        sad_cow_bin_entry_id as inventory_log_id
        ,sku_vendor_id
        ,'SAD_COW_DELIVER' as reason
        ,sku_id
        ,updated_at_utc
        ,lot_number
        ,sku_box_id
        ,fc_id
        ,user_id
        ,created_at_utc
        ,sad_cow_bin_entry_id
        ,owner_id as inventory_owner_id
        ,sku_quantity
        ,null::int as order_id
    from get_sad_cow_receiving_log
)

,get_vendor_names as (
    select
        union_logs.*
        ,vendor.sku_vendor_name 
        ,sku_owner.sku_vendor_name as sku_owner_name
        ,sku_owner.is_marketplace
        ,sku_owner.is_rastellis
    from union_logs
        left join sku_vendor as vendor on union_logs.sku_vendor_id = vendor.sku_vendor_id
        left join sku_vendor as sku_owner on union_logs.inventory_owner_id = sku_owner.sku_vendor_id
)

,get_join_keys as (
    select
        get_vendor_names.*
        ,fc.fc_key
        ,sku.sku_key
    from get_vendor_names
        left join fc on get_vendor_names.fc_id = fc.fc_id
            and get_vendor_names.created_at_utc >= fc.adjusted_dbt_valid_from
            and get_vendor_names.created_at_utc < fc.adjusted_dbt_valid_to
        left join sku on get_vendor_names.sku_id = sku.sku_id
            and get_vendor_names.created_at_utc >= sku.adjusted_dbt_valid_from
            and get_vendor_names.created_at_utc < sku.adjusted_dbt_valid_to
)

select 
    {{ dbt_utils.surrogate_key(['inventory_log_id','sad_cow_bin_entry_id','reason']) }} as inventory_id
    ,* 
from get_join_keys
