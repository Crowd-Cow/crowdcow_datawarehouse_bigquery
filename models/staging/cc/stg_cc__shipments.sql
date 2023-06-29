with 

source as ( select * from {{ source('cc', 'shipments') }} where not _fivetran_deleted )
,fc_outbound_pallets as (select * from {{ source('cc', 'fc_outbound_pallets')}} where not _fivetran_deleted )

,renamed as (
    select
        id as shipment_id
        ,{{ clean_strings('latest_temperature_icon') }} as latest_temperature_icon
        ,ready_for_pickup_at as ready_for_pickup_at_utc
        ,easypost_tracking_code as shipment_tracking_code
        ,{{ clean_strings('easypost_postage_carrier') }} as shipment_postage_carrier
        ,print_queue_item_id
        ,delivery_window_end as delivery_window_end_at_utc
        ,{{ cents_to_usd('easypost_postage_rate_cents') }}as shipment_postage_rate_usd
        ,mean_temperature
        ,box_type_id
        ,easypost_shipment_id
        ,delivery_window_start as delivery_window_start_at_utc
        ,{{ clean_strings('easypost_shipping_label_url') }} as easypost_shipping_label_url
        ,shipped_at as shipped_at_utc
        ,delivered_at as delivered_at_utc
        ,convert_timezone('UTC','America/Los_Angeles',delivered_at) as delivered_at_pt
        ,{{ clean_strings('delivery_method') }} as delivery_method
        ,easypost_original_estimated_delivery_date as original_est_delivery_date_utc
        ,convert_timezone('UTC','America/Los_Angeles',easypost_original_estimated_delivery_date) as original_est_delivery_date_pt
        ,easypost_postage_rate_id
        ,marked_not_shipped_at as marked_not_shipped_at_utc
        ,fc_id
        ,maximum_temperature
        ,temperature_last_updated_at as temperature_last_updated_at_utc
        ,easypost_postage_label_id as shipment_postage_label_id
        ,postage_paid_at as postage_paid_at_utc
        ,item_weight as shipment_weight
        ,scanned_box_type_id
        ,easypost_estimated_delivery_date as est_delivery_date_utc
        ,{{ cents_to_usd('packaging_freight_component_cost_cents') }} as packaging_freight_component_cost_usd
        ,minimum_temperature
        ,scheduled_fulfillment_date as scheduled_fulfillment_date_utc
        ,order_id
        ,latest_tracking_details_updated_at as latest_tracking_details_updated_at_utc
        ,available_for_pickup_at as available_for_pickup_at_utc
        ,created_at as created_at_utc
        ,easypost_delivery_days as shipment_delivery_days
        ,lost_at as lost_at_utc
        ,{{ clean_strings('pickup_at_description') }} as pickup_at_description
        ,updated_at as updated_at_utc
        ,{{ clean_strings('easypost_postage_service') }} as shipment_postage_service
        ,{{ cents_to_usd('packaging_materials_component_cost_cents') }} as packaging_materials_component_cost_usd
        ,token as shipment_token
        ,{{ clean_strings('shipment_notes') }} as shipment_notes
        ,latest_temperature
        ,use_zpl as does_use_zpl
        ,receives_tracking_updates as does_receive_tracking_updates
        ,easypost_delivery_date_guaranteed as is_delivery_date_guaranteed
        ,fc_locations_id
        ,fc_location_id

    from source

)
/**** outbound pallets shipping option name  ****/
    ,outbound_pallets as (
    select 
        source.id as shipment_id,
        fc_outbound_pallets.shipping_option_name as shipping_option_name
    from source 
    join fc_outbound_pallets 
    on fc_outbound_pallets.fc_location_id = source.fc_location_id 
    --and fc_outbound_pallets.shipping_option_name IN ('LAX LH UPS', 'ATL LH UPS')
    --and fc_outbound_pallets.shipping_carrier = 'UPS'
)

,clean_axlehire_costs as (
    select
        renamed.shipment_id
        ,latest_temperature_icon
        ,ready_for_pickup_at_utc
        ,shipment_tracking_code
        ,shipment_postage_carrier
        ,print_queue_item_id
        ,delivery_window_end_at_utc
        
        ,iff(
           (shipment_postage_carrier = 'AXLEHIREV3' and shipment_postage_rate_usd = 0.01) or
           (shipment_postage_carrier = 'AXLEHIREV3' and shipped_at_utc < '2021-12-01')
           ,null
           ,shipment_postage_rate_usd
        ) as shipment_postage_rate_usd
        
        ,mean_temperature
        ,box_type_id
        ,easypost_shipment_id
        ,delivery_window_start_at_utc
        ,easypost_shipping_label_url
        ,shipped_at_utc
        ,delivered_at_utc
        ,delivered_at_pt
        ,delivery_method
        ,original_est_delivery_date_utc
        ,original_est_delivery_date_pt
        ,easypost_postage_rate_id
        ,marked_not_shipped_at_utc
        ,fc_id
        ,maximum_temperature
        ,temperature_last_updated_at_utc
        ,shipment_postage_label_id
        ,postage_paid_at_utc
        ,shipment_weight
        ,scanned_box_type_id
        ,est_delivery_date_utc
        ,packaging_freight_component_cost_usd
        ,minimum_temperature
        ,scheduled_fulfillment_date_utc
        ,order_id
        ,latest_tracking_details_updated_at_utc
        ,available_for_pickup_at_utc
        ,created_at_utc
        ,shipment_delivery_days
        ,lost_at_utc
        ,pickup_at_description
        ,updated_at_utc
        ,shipment_postage_service
        ,packaging_materials_component_cost_usd
        ,shipment_token
        ,shipment_notes
        ,latest_temperature
        ,does_use_zpl
        ,does_receive_tracking_updates
        ,is_delivery_date_guaranteed
        ,fc_locations_id
        ,renamed.fc_location_id
        ,outbound_pallets.shipping_option_name
    from renamed
    left join outbound_pallets on outbound_pallets.shipment_id = renamed.shipment_id
)

select * from clean_axlehire_costs

