with 

source as ( select * from {{ source('cc', 'shipments') }} where not _fivetran_deleted )

,renamed as (
    select
        id as shipment_id
        ,{{ clean_strings('latest_temperature_icon') }} as latest_temperature_icon
        ,ready_for_pickup_at as ready_for_pickup_at_utc
        ,easypost_tracking_code
        ,{{ clean_strings('easypost_postage_carrier') }} as easypost_postage_carrier
        ,print_queue_item_id
        ,delivery_window_end as delivery_window_end_at_utc
        ,nullif({{ cents_to_usd('easypost_postage_rate_cents') }},0.01) as easypost_postage_rate_usd
        ,mean_temperature
        ,box_type_id
        ,easypost_shipment_id
        ,delivery_window_start as delivery_window_start_at_utc
        ,{{ clean_strings('easypost_shipping_label_url') }} as easypost_shipping_label_url
        ,shipped_at as shipped_at_utc
        ,delivered_at as delivered_at_utc
        ,{{ clean_strings('delivery_method') }} as delivery_method
        ,easypost_original_estimated_delivery_date as easypost_original_estimated_delivery_date_utc
        ,easypost_postage_rate_id
        ,marked_not_shipped_at as marked_not_shipped_at_utc
        ,fc_id
        ,maximum_temperature
        ,temperature_last_updated_at as temperature_last_updated_at_utc
        ,easypost_postage_label_id
        ,postage_paid_at as postage_paid_at_utc
        ,item_weight
        ,scanned_box_type_id
        ,easypost_estimated_delivery_date as easypost_estimated_delivery_date_utc
        ,{{ cents_to_usd('packaging_freight_component_cost_cents') }} as packaging_freight_component_cost_usd
        ,minimum_temperature
        ,scheduled_fulfillment_date as scheduled_fulfillment_date_utc
        ,order_id
        ,latest_tracking_details_updated_at as latest_tracking_details_updated_at_utc
        ,available_for_pickup_at as available_for_pickup_at_utc
        ,created_at as created_at_utc
        ,easypost_delivery_days
        ,lost_at as lost_at_utc
        ,{{ clean_strings('pickup_at_description') }} as pickup_at_description
        ,updated_at as updated_at_utc
        ,{{ clean_strings('easypost_postage_service') }} as easypost_postage_service
        ,{{ cents_to_usd('packaging_materials_component_cost_cents') }} as packaging_materials_component_cost_usd
        ,token as shipment_token
        ,{{ clean_strings('shipment_notes') }} as shipment_notes
        ,latest_temperature
        ,use_zpl as does_use_zpl
        ,receives_tracking_updates as does_receive_tracking_updates
        ,easypost_delivery_date_guaranteed as is_easypost_delivery_date_guaranteed
        ,fc_locations_id
        ,fc_location_id

    from source

)

select * from renamed

