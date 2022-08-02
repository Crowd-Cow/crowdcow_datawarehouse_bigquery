with

shipment as ( select * from {{ ref('stg_cc__shipments') }} )
,fc as ( select * from {{ ref('stg_cc__fcs') }} )
,order_delivery as ( select * from {{ ref('stg_cc__orders') }} )

,get_fc_key as (
    select
        shipment.*
        ,fc.fc_key
    from shipment
        left join fc on shipment.fc_id = fc.fc_id
            and shipment.shipped_at_utc >= fc.adjusted_dbt_valid_from
            and shipment.shipped_at_utc < fc.adjusted_dbt_valid_to
)

,get_order_delivery_address as (
    select
        get_fc_key.*
        ,order_delivery.order_delivery_state
        ,order_delivery.order_delivery_postal_code
        ,order_delivery.coolant_weight_in_pounds
    from get_fc_key
        left join order_delivery on get_fc_key.order_id = order_delivery.order_id
)

,calc_axlehire_default as (
    select distinct
        shipment_postage_carrier
        ,order_delivery_state
        ,order_delivery_postal_code
        ,avg(shipment_postage_rate_usd) over() as axlehire_default_rate_month
        ,avg(shipment_postage_rate_usd) over(partition by order_delivery_state) as axlehire_default_rate_state
        ,avg(shipment_postage_rate_usd) over(partition by order_delivery_postal_code) as axlehire_default_rate_postal_code
    from get_order_delivery_address
    where shipment_postage_carrier = 'AXLEHIREV3'
        and datediff(month,shipped_at_utc,sysdate()) <= 2
)

,add_default_cost as (
    select
        get_order_delivery_address.shipment_id
        ,get_order_delivery_address.print_queue_item_id
        ,get_order_delivery_address.box_type_id
        ,get_order_delivery_address.scanned_box_type_id
        ,get_order_delivery_address.fc_id
        ,get_order_delivery_address.fc_key
        ,get_order_delivery_address.order_id
        ,get_order_delivery_address.fc_location_id
        ,get_order_delivery_address.shipment_token
        ,get_order_delivery_address.shipment_tracking_code
        ,get_order_delivery_address.shipment_postage_carrier
        
        ,coalesce(
            get_order_delivery_address.shipment_postage_rate_usd
            ,calc_axlehire_default.axlehire_default_rate_postal_code
            ,calc_axlehire_default.axlehire_default_rate_state
            ,calc_axlehire_default.axlehire_default_rate_month
        ) as shipment_postage_rate_usd

        ,div0(
            get_order_delivery_address.coolant_weight_in_pounds
            ,count(get_order_delivery_address.shipment_id) over(partition by get_order_delivery_address.order_id)
        ) as coolant_pounds_per_shipment

        ,get_order_delivery_address.delivery_method
        ,get_order_delivery_address.item_weight
        ,get_order_delivery_address.packaging_freight_component_cost_usd
        ,get_order_delivery_address.shipment_delivery_days
        
        ,get_order_delivery_address.delivered_at_utc::date 
            - coalesce(shipped_at_utc,scheduled_fulfillment_date_utc)::date as actual_transit_days
        
        ,get_order_delivery_address.pickup_at_description
        ,get_order_delivery_address.shipment_postage_service
        ,get_order_delivery_address.packaging_materials_component_cost_usd
        ,get_order_delivery_address.order_delivery_postal_code
        ,get_order_delivery_address.does_use_zpl
        ,get_order_delivery_address.does_receive_tracking_updates
        ,get_order_delivery_address.is_delivery_date_guaranteed
        ,get_order_delivery_address.shipped_at_utc
        ,get_order_delivery_address.delivered_at_utc
        ,get_order_delivery_address.original_est_delivery_date_utc
        ,get_order_delivery_address.original_est_delivery_date_utc + interval '20 hours' as delivery_cutoff_at_utc
        ,get_order_delivery_address.delivered_at_utc > delivery_cutoff_at_utc as is_delivery_late
        ,get_order_delivery_address.est_delivery_date_utc
        ,get_order_delivery_address.postage_paid_at_utc
        ,get_order_delivery_address.scheduled_fulfillment_date_utc
        ,get_order_delivery_address.latest_tracking_details_updated_at_utc
        ,get_order_delivery_address.available_for_pickup_at_utc
        ,get_order_delivery_address.created_at_utc
        ,get_order_delivery_address.updated_at_utc
        ,get_order_delivery_address.lost_at_utc
    from get_order_delivery_address
        left join calc_axlehire_default on get_order_delivery_address.shipment_postage_carrier = calc_axlehire_default.shipment_postage_carrier
            and get_order_delivery_address.order_delivery_postal_code = calc_axlehire_default.order_delivery_postal_code
)

select * from add_default_cost
