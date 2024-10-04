with 

source as ( select * from {{ source('cc', 'shipment_plans') }} )

,renamed as (
    select 
        id as shipment_plan_id 
        ,created_at as created_at_utc
        ,updated_at as updated_at_utc
        ,scheduled_fulfillment_date as scheduled_fulfillment_date_utc
        ,scheduled_arrival_date as scheduled_arrival_date_utc
        ,shipping_service 
        ,transit_days
        ,order_id
        ,shipping_option_id
    from source
)

select * from renamed 