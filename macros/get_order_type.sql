{%- macro get_order_type(table_name) -%}
    
    (
        select max( order_type ) as order_type 
            from staging.stg_cc__orders
            where orders.order_id = {{ table_name }}.order_id
    )
  
{%- endmacro -%}