{%- macro get_order_type(table_name) -%}
    (
        select max( order_type ) as order_type 
            from business_vault.orders
            where orders.order_id = {{ table_name }}.order_id
    )
  
{%- endmacro -%}