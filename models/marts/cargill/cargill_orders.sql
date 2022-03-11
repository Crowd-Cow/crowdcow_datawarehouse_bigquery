with

order_info as ( select * from {{ ref('orders') }} )
,order_item_detail as ( select * from {{ ref('order_item_details') }} )
,sku as ( select * from {{ ref('skus') }} )

,cargill_order_items as (
    select distinct
        order_item_detail.order_id
    from order_item_detail
        left join sku on order_item_detail.sku_key = sku.sku_key
    where sku.is_cargill
)

,cargill_orders as (
    select
        order_info.*
    from order_info
        inner join cargill_order_items on order_info.order_id = cargill_order_items.order_id
)

select 
    order_id
    ,parent_order_id
    ,order_token
    ,user_id
    ,subscription_id
    ,fc_id
    ,visit_id
    --,stripe_charge_id
    ,fc_key
    ,order_identifier
    ,order_current_state
    ,order_type
    ,stripe_failure_code
    --,order_delivery_street_address_1
    --,order_delivery_street_address_2
    ,order_delivery_city
    ,order_delivery_state
    ,order_delivery_postal_code
    --,billing_address_1
    --,billing_address_2
    ,billing_city
    ,billing_state
    ,billing_postal_code
    ,gross_product_revenue
    ,membership_discount
    ,merch_discount
    ,free_protein_promotion
    ,net_product_revenue
    ,shipping_revenue
    ,free_shipping_discount
    ,gross_revenue
    ,new_member_discount
    ,refund_amount
    ,gift_redemption
    ,other_discount
    ,net_revenue
    ,product_cost
    ,coolant_weight_in_pounds
    ,order_bids_count
    ,has_free_shipping
    ,is_ala_carte_order
    ,is_membership_order
    ,is_completed_order
    ,is_paid_order
    ,is_cancelled_order
    ,is_abandonded_order
    ,is_gift_order
    ,is_bulk_gift_order
    ,is_gift_card_order
    ,has_shipped
    ,has_been_delivered
    ,is_fulfillment_risk
    ,overall_order_rank
    ,completed_order_rank
    ,paid_order_rank
    ,cancelled_order_rank
    ,ala_carte_order_rank
    ,completed_ala_carte_order_rank
    ,paid_ala_carte_order_rank
    ,cancelled_ala_carte_order_rank
    ,membership_order_rank
    ,completed_membership_order_rank
    ,paid_membership_order_rank
    ,cancelled_membership_order_rank
    ,unique_membership_order_rank
    ,completed_unique_membership_order_rank
    ,paid_unique_membership_order_rank
    ,cancelled_unique_membership_order_rank
    ,gift_order_rank
    ,completed_gift_order_rank
    ,paid_gift_order_rank
    ,cancelled_gift_order_rank
    ,gift_card_order_rank
    ,completed_gift_card_order_rank
    ,paid_gift_card_order_rank
    ,cancelled_gift_card_order_rank
    ,beef_units
    ,bison_units
    ,chicken_units
    ,desserts_units
    ,duck_units
    ,game_meat_units
    ,japanese_wagyu_units
    ,lamb_units
    ,pet_food_units
    ,plant_based_proteins_units
    ,pork_units
    ,salts_seasonings_units
    ,seafood_units
    ,starters_sides_units
    ,turkey_units
    ,wagyu_units
    ,bundle_units
    ,total_units
    ,pct_beef
    ,pct_bison
    ,pct_chicken
    ,pct_desserts
    ,pct_duck
    ,pct_game_meat
    ,pct_japanese_wagyu
    ,pct_lamb
    ,pct_pet_food
    ,pct_plant_based_proteins
    ,pct_pork
    ,pct_salts_seasonings
    ,pct_seafood
    ,pct_starters_sides
    ,pct_turkey
    ,pct_wagyu
    ,pct_bundle
    ,order_created_at_utc
    ,order_updated_at_utc
    ,order_checkout_completed_at_utc
    ,order_cancelled_at_utc
    ,order_paid_at_utc
    ,order_first_stuck_at_utc
    ,customer_viewed_at_utc
    ,next_box_notified_at_utc
    ,order_scheduled_fulfillment_date_utc
    ,order_scheduled_arrival_date_utc
    ,shipped_at_utc
    ,delivered_at_utc
from cargill_orders