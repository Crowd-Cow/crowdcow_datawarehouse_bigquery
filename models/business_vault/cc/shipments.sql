with

shipment as ( select * from {{ ref('stg_cc__shipments') }} )
,fc as ( select * from {{ ref('stg_cc__fcs') }} )

,get_fc_key as (
    select
        shipment.*
        ,fc.fc_key
    from shipment
        left join fc on shipment.fc_id = fc.fc_id
            and shipment.shipped_at_utc >= fc.adjusted_dbt_valid_from
            and shipment.shipped_at_utc < fc.adjusted_dbt_valid_to
)

select
    shipment_id
    ,print_queue_item_id
    ,box_type_id
    ,scanned_box_type_id
    ,fc_id
    ,fc_key
    ,order_id
    ,fc_location_id
    ,shipment_token
    ,easypost_tracking_code as shipment_tracking_code
    ,easypost_postage_carrier as shipment_postage_carrier
    ,easypost_postage_rate_usd as shipment_postage_rate_usd
    ,delivery_method
    ,item_weight
    ,packaging_freight_component_cost_usd
    ,easypost_delivery_days as shipment_delivery_days
    ,pickup_at_description
    ,easypost_postage_service as shipment_postage_service
    ,packaging_materials_component_cost_usd
    ,does_use_zpl
    ,does_receive_tracking_updates
    ,is_easypost_delivery_date_guaranteed as is_delivery_date_guaranteed
    ,shipped_at_utc
    ,delivered_at_utc
    ,easypost_original_estimated_delivery_date_utc as original_est_delivery_date_utc
    ,easypost_estimated_delivery_date_utc as est_delivery_date_utc
    ,postage_paid_at_utc
    ,scheduled_fulfillment_date_utc
    ,latest_tracking_details_updated_at_utc
    ,available_for_pickup_at_utc
    ,created_at_utc
    ,updated_at_utc
    ,lost_at_utc
from get_fc_key
