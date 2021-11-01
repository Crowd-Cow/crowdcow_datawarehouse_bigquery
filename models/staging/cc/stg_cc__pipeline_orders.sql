with source as (

    select * from {{ ref('pipeline_orders_ss')}} where not _fivetran_deleted 

),

renamed as (

    select
        id as pipeline_order_id
        , special_instructions_changed
        , events_completed_at as events_completed_at_utc
        , inventory_owner_id
        , expected_pounds
        , box_count
        , pipeline_actor_id
        , last_updated_by
        , lot_number
        , processor_loss_pounds
        , updated_at as updated_at_utc
        , processor_freight_id
        , created_by 
        , freight_order_number
        , packer_total_shipment_weight
        , {{ clean_strings('special_instructions') }} as special_instructions 
        , pallet_count
        , {{ clean_strings('processor_bol_url') }} as processor_bol_url
        , {{ clean_strings('farm_bol_url') }} as farm_bol_url
        , fc_box_count
        , lot_cost_applied_at as lot_cost_applied_at_utc
        , fc_freight_id
        , packer_box_count
        , kill_date
        , quantity
        , {{ clean_strings('payment_terms') }} as payment_terms
        , {{ clean_strings('admin_notes') }} as admin_notes
        , {{ clean_strings('pipeline_order_type') }} as pipeline_order_type
        , created_at as created_at_utc
        , boxed_beef_processor_id
        , {{ clean_strings('packer_bol_url') }} as packer_bol_url
        , {{ cents_to_usd('potential_revenue_cents') }} as potential_revenue_usd
        , processor_total_shipment_weight
        , {{ clean_strings('coa_url') }} as coa_url
        , processor_delivery_scheduled as is_processor_delivery_scheduled
        , debut_lot as is_debut_lot
        , marketplace as is_marketplace
        , removed as is_removed
        , fc_delivery_scheduled as is_fc_delivery_scheduled
        , dbt_valid_to
        , dbt_valid_from

    from source

)

select * from renamed

