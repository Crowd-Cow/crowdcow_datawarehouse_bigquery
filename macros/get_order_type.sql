{%- macro get_order_type(table_name) -%}
    
    (
        select max( is_rastellis ) as is_rastellis
            from staging.stg_cc__orders
            where stg_cc__orders.order_id = {{ table_name }}.order_id
    )
  
{%- endmacro -%}
