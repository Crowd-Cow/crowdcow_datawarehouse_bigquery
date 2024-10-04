with source as (

    select * from {{ source('cc', 'pipeline_receivables') }} 

),

renamed as (

    select
        id as pipeline_receivables_id
        ,sku_id
        ,quantity as quantity_ordered
        ,marked_destroyed_at as marked_destroyed_at_utc
        --,cut_id
        --,weight
        ,created_at as created_at_utc
        ,quantity_received
        ,received_at as received_at_utc
        ,pipeline_order_id
        ,updated_at as updated_at_utc
        ,{{ cents_to_usd('cost_in_cents') }} as cost_per_unit_usd
    from source

)

select * from renamed

