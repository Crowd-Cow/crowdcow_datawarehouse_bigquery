with

tracking_details as ( select * from {{ ref('stg_cc__tracking_details') }} )

,arrived_at_facility as (
    select shipment_id
        ,city
        ,state
        ,tracking_updated_at_utc as arrived_at_utc
    from tracking_details
    where (message like 'ARRIVED%' or message like 'ORIGIN%')
)

,departed_facility as (
        select shipment_id
        ,city
        ,state
        ,tracking_updated_at_utc as departed_at_utc
    from tracking_details
    where (message like 'DEPARTED%' or message like 'OUT FOR DELIVERY%')
)

,combined as (
    select arrived_at_facility.shipment_id
        ,arrived_at_facility.city
        ,arrived_at_facility.state
        ,arrived_at_facility.arrived_at_utc
        ,departed_facility.departed_at_utc
    from arrived_at_facility
        left join departed_facility on arrived_at_facility.shipment_id = departed_facility.shipment_id
                                    and arrived_at_facility.city = departed_facility.city
                                    and arrived_at_facility.state = departed_facility.state
)

select *
from combined