with 

source as ( select * from {{ source('cc', 'shipping_options') }} )

,renamed as (
    select
        id as shipping_option_id
        ,{{ clean_strings('name') }} as order_shipping_option_name
        ,service as service
        ,created_at as created_at_utc 
        ,updated_at as updated_at_utc
        ,carrier as carrier
        ,transit_days 
    from source 
)

select * from renamed


