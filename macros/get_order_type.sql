{%- macro get_order_type(table_name) -%}
    
    ( 
            select max( is_rastellis ) as is_rastellis
            from {{ ref('stg_cc__orders') }} as staging_orders
            where staging_orders.order_id = {{ table_name }}.order_id
    )
  
{%- endmacro -%}
