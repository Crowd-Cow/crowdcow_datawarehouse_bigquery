with

shipment as ( select * from {{ ref('stg_cc__shipments') }} )

select
    order_id
    ,any_value(shipment_postage_carrier) as shipment_postage_carrier
    ,count(distinct shipment_id) as shipment_count
    ,max(lost_at_utc) as lost_at_utc
    ,max(shipped_at_utc) as shipped_at_utc
    ,max(delivered_at_utc) as delivered_at_utc
from shipment
group by 1
