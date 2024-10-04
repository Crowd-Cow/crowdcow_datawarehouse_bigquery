with

shipped_orders as ( select * from {{ ref('orders') }} where has_shipped and order_type = 'E-COMMERCE'and gift_redemption = 0 and other_discount = 0)

,get_margin_drivers as (
    select
        order_id
        ,order_type
        ,user_id
        ,fc_id
        ,fc_key
        ,order_delivery_state
        ,product_profit
        ,gross_profit
        ,if(net_revenue < 0,0,net_revenue) as net_revenue
        ,-membership_discount as membership_discount
        ,-merch_discount as merch_discount
        ,-free_protein_promotion as free_protein_promotion
        ,-free_shipping_discount as free_shipping_discount
        ,-new_member_discount as new_member_discount
        ,-refund_amount as refund_amount
        ,-moolah_item_discount as moolah_item_discount
        ,-moolah_order_discount as moolah_order_discount
        ,net_product_revenue
        ,product_cost
        ,shipment_cost
        ,coolant_cost
        ,packaging_cost
        ,care_cost
        ,fc_labor_cost
        ,inbound_shipping_cost
        ,shipment_count
        ,total_units
        ,total_product_weight
        ,shipped_at_utc
    from shipped_orders
)

select * from get_margin_drivers
