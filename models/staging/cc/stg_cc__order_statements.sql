with source as (

    select * from {{ source('cc', 'order_statements') }}

),

renamed as (

    select
        id as order_statement_id
        ,{{ cents_to_usd('marketing_inserts') }} as marketing_insert_cost_usd
        ,{{ cents_to_usd('net_revenue') }} as net_revenue_usd
        ,{{ cents_to_usd('contribution_margin_before_marketing') }} as contribution_margin_before_marketing_usd
        ,{{ convert_percent('contribution_margin_before_marketing_percentage', 6) }} as contribution_margin_before_marketing_percentage
        ,{{ cents_to_usd('freight_out') }} as freight_out_usd
        ,{{convert_percent('net_revenue_percentage', 6) }} as net_revenue_percentage
        ,shipments_count
        ,{{ cents_to_usd('product_revenue_cents') }} as product_revenue_usd
        ,{{ cents_to_usd('total_payments') }} as total_payments_usd
        ,{{ cents_to_usd('refunds_cents') }} refund_amount_usd
        ,{{ cents_to_usd('product_cost') }} as product_cost_usd
        ,{{ convert_percent('product_margin_percentage', 6) }} as product_margin_percentage
        ,line_item_count
        ,{{ cents_to_usd('fulfillment_cost') }} as fulfillment_cost_usd
        ,{{ cents_to_usd('total_discounts') }} as total_discount_amount_usd
        ,{{ cents_to_usd('payment_processing') }} as payment_processing_amount_usd
        ,{{ cents_to_usd('product_margin') }} as product_margin_usd
        ,order_id
        ,{{ cents_to_usd('customer_service_cost') }} as customer_service_cost_usd
        ,created_at as created_at_utc
        ,{{ cents_to_usd('gross_revenue') }} as gross_revenue_usd
        ,{{ convert_percent('contribution_margin_percentage', 6) }} as contribution_margin_percentage
        ,{{ cents_to_usd('gross_margin') }} as gross_margin_usd
        ,{{ convert_percent('gross_margin_percentage', 6) }} as gross_margin_percentage
        ,{{ cents_to_usd('freight_revenue_cents') }} as freight_revenue_usd
        ,{{ cents_to_usd('contribution_margin') }} as contribution_margin_usd
        ,{{ convert_percent('total_discounts_percentage', 6) }} as total_discounts_percentage
        ,updated_at as updated_at_utc

    from source

)

select * from renamed

