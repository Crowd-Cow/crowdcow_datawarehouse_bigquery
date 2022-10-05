with

shipment as ( select * from {{ ref('shipments') }} )

select
    order_id
    ,any_value(shipment_postage_carrier) as shipment_postage_carrier
    ,count(distinct shipment_id) as shipment_count
    ,max(lost_at_utc) as lost_at_utc
    ,max(shipped_at_utc) as shipped_at_utc
    ,max(delivered_at_utc) as delivered_at_utc
    ,max(original_est_delivery_date_utc) as original_est_delivery_date_utc
    ,max(est_delivery_date_utc) as est_delivery_date_utc
    ,avg(delivery_days_late) as delivery_days_late
    ,listagg(shipment_tracking_code,' | ') as shipment_tracking_code_list
from shipment
group by 1
