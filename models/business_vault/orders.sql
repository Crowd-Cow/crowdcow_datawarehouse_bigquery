with

orders as ( select * from {{ ref('stg_cc__orders') }} )
,order_revenue as ( select * from {{ ref('int_order_revenue') }} )
,flags as ( select * from {{ ref('int_order_flags') }} )
,ranks as ( select * from {{ ref('int_order_ranks') }} )
,units as ( select * from {{ ref('int_order_units_pct') }} )

,order_joins as (
    select
        orders.order_id
        ,orders.parent_order_id
        ,orders.order_token
        ,orders.user_id
        ,orders.subscription_id
        ,orders.fc_id
        ,orders.visit_id
        ,orders.order_type
        ,orders.stripe_failure_code
        ,orders.order_delivery_street_address_1
        ,orders.order_delivery_street_address_2
        ,orders.order_delivery_city
        ,orders.order_delivery_state
        ,orders.order_delivery_postal_code
        ,orders.billing_address_1
        ,orders.billing_address_2
        ,orders.billing_city
        ,orders.billing_state
        ,orders.billing_postal_code
        ,zeroifnull(order_revenue.gross_revenue_usd) as gross_revenue_usd
        ,zeroifnull(order_revenue.product_revenue_usd) as product_revenue_usd
        ,zeroifnull(order_revenue.order_shipping_fee_usd) as shipping_revenue_usd    
        ,zeroifnull(order_revenue.discount_percent) as discount_percent
        ,zeroifnull(order_revenue.discount_amount_usd) as discount_amount_usd
        ,zeroifnull(order_revenue.refund_amount_usd) as refund_amount_usd
        ,zeroifnull(order_revenue.net_revenue_usd) as net_revenue_usd
        ,flags.has_free_shipping
        ,flags.is_ala_carte_order
        ,flags.is_membership_order
        ,flags.is_completed_order
        ,flags.is_paid_order
        ,flags.is_cancelled_order
        ,flags.is_abandonded_order
        ,flags.is_gift_order
        ,flags.is_bulk_gift_order
        ,flags.is_gift_card_order
        ,ranks.overall_order_rank
        ,ranks.completed_order_rank
        ,ranks.paid_order_rank
        ,ranks.cancelled_order_rank
        ,ranks.ala_carte_order_rank
        ,ranks.completed_ala_carte_order_rank
        ,ranks.paid_ala_carte_order_rank
        ,ranks.cancelled_ala_carte_order_rank
        ,ranks.membership_order_rank
        ,ranks.completed_membership_order_rank
        ,ranks.paid_membership_order_rank
        ,ranks.cancelled_membership_order_rank
        ,ranks.unique_membership_order_rank
        ,ranks.completed_unique_membership_order_rank
        ,ranks.paid_unique_membership_order_rank
        ,ranks.cancelled_unique_membership_order_rank
        ,ranks.gift_order_rank
        ,ranks.completed_gift_order_rank
        ,ranks.paid_gift_order_rank
        ,ranks.cancelled_gift_order_rank
        ,ranks.gift_card_order_rank
        ,ranks.completed_gift_card_order_rank
        ,ranks.paid_gift_card_order_rank
        ,ranks.cancelled_gift_card_order_rank
        ,orders.order_created_at_utc
        ,orders.order_updated_at_utc
        ,orders.order_checkout_completed_at_utc
        ,orders.order_cancelled_at_utc
        ,orders.order_paid_at_utc
        ,orders.order_first_stuck_at_utc
        ,orders.order_scheduled_fulfillment_date_utc
        ,orders.order_scheduled_arrival_date_utc
        ,units.beef_units
        ,units.bison_units
        ,units.chicken_units
        ,units.desserts_units
        ,units.duck_units
        ,units.game_meat_units
        ,units.japanese_wagyu_units
        ,units.lamb_units
        ,units.pet_food_units
        ,units.plant_based_proteins_units
        ,units.pork_units
        ,units.salts_seasonings_units
        ,units.seafood_units
        ,units.starters_sides_units
        ,units.turkey_units
        ,units.wagyu_units
        ,units.bundle_units
        ,units.total_units
        ,units.pct_beef
        ,units.pct_bison
        ,units.pct_chicken
        ,units.pct_desserts
        ,units.pct_duck
        ,units.pct_game_meat
        ,units.pct_japanese_wagyu
        ,units.pct_lamb
        ,units.pct_pet_food
        ,units.pct_plant_based_proteins
        ,units.pct_pork
        ,units.pct_salts_seasonings
        ,units.pct_seafood
        ,units.pct_starters_sides
        ,units.pct_turkey
        ,units.pct_wagyu
        ,units.pct_bundle
    from orders
        left join order_revenue on orders.order_id = order_revenue.order_id
        left join flags on orders.order_id = flags.order_id
        left join ranks on orders.order_id = ranks.order_id
        left join units on orders.order_id = units.order_id
)

select * from order_joins
