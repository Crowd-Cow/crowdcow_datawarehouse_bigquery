with 
    source as (select * from {{ source('google_sheets', 'q4_forecast') }} )

    ,renamed as (
        select
            week_start,
            fiscal_week,
            corporate_gifting,
            cumulative_turkey_order_revenue,
            existing_customer_alc_gifting,
            gift_cards,
            new_alc_aov,
            new_alc_net_sales,
            new_alc_orders,
            new_customer_alc_gifting,
            new_customers_aov,
            new_customers_net_sales,
            new_customers_orders,
            new_subscriber_aov,
            new_subscriber_net_sales,
            new_subscriber_orders,
            subscriber_alc_gifting,
            total_net_sales,
            total_non_subscriber_net_sales,
            total_subscriber_net_sales,
            total_turkey_order_revenue,
            turkey_add_on_revenue,
            turkey_cumulative_add_on_revenue,
            turkey_cumulative_revenue,
            turkey_cumulative_units,
            turkey_revenue,
            turkey_units_sold,
            
        FROM source 
    )

    select * from renamed 