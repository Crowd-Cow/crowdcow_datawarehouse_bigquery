{{
  config(
    snowflake_warehouse = 'TRANSFORMING_M'
    )
}}

with

orders as ( select * from {{ ref('stg_cc__orders') }} )
,order_revenue as ( select * from {{ ref('int_order_revenue') }} )
,order_cost as ( select * from {{ ref('int_order_costs') }} )
,flags as ( select * from {{ ref('int_order_flags') }} )
,ranks as ( select * from {{ ref('int_order_ranks') }} )
,units as ( select * from {{ ref('int_order_units_pct') }} )
,order_shipment as ( select * from {{ ref('int_order_shipments') }} )
,order_reschedule as ( select * from {{ ref('int_order_reschedules') }} )
,order_promo_redeemed as ( select * from {{ ref('int_partner_promo_redemptions') }} )
,order_failure as ( select * from {{ ref('int_order_failures') }} )
,reward as ( select * from {{ ref('int_order_rewards') }} )
,charges as (select * from {{ ref('stg_stripe__charges') }} )
,payment_method_card as (select * from {{ ref('stg_stripe__payment_method_card') }} )
,shipping_options as (select * from {{ ref('stg_cc__shipping_options') }} )
,shipment_plans as (select * from {{ ref('stg_cc__shipment_plans')}} )
,user_membership as ( select * from {{ ref('stg_cc__subscriptions') }} where user_id is not null)

,payment_methods as (
    select order_id
        ,created_at_utc 
        ,wallet_type
    from orders
        left join charges on orders.stripe_charge_id = charges.stripe_charge_id
        left join payment_method_card on charges.payment_method_id = payment_method_card.payment_method_id
)

,order_shipment_option as (
    select 
        order_id
        ,order_shipping_option_name
        ,shipment_plans.transit_days
    from shipment_plans
    left join shipping_options on shipment_plans.shipping_option_id = shipping_options.shipping_option_id
)

,order_joins as (
    select
        orders.order_id
        ,orders.parent_order_id
        ,orders.order_token
        ,orders.user_id
        ,orders.subscription_id
        ,orders.fc_id
        ,orders.visit_id
        ,orders.packer_id
        ,orders.stripe_charge_id
        ,payment_methods.created_at_utc as stripe_charge_created_at_utc
        ,payment_methods.wallet_type
        ,order_promo_redeemed.partner_id
        ,order_promo_redeemed.partner_key
        ,{{ get_join_key('stg_cc__fcs','fc_key','fc_id','orders','fc_id','order_updated_at_utc') }} as fc_key
        ,orders.order_identifier
        ,order_failure.failure_reasons
        ,orders.order_current_state
        ,order_reschedule.reschedule_reason
        ,orders.third_party_internal_identifier 
        ,orders.third_party_customer_identifier

        ,case
            when orders.parent_order_id is not null then 'CORP GIFT'
            when orders.order_type = 'REPLACEMENT' and flags.is_bulk_gift_order then 'CORP GIFT REPLACEMENT'
            else orders.order_type
         end as order_type
        
        ,orders.stripe_failure_code
        ,orders.stripe_card_brand
        ,orders.order_delivery_street_address_1
        ,orders.order_delivery_street_address_2
        ,orders.order_delivery_city
        ,orders.order_delivery_state
        ,orders.order_delivery_postal_code
        ,orders.order_delivery_county_name
        ,orders.dma_name
        ,orders.billing_address_1
        ,orders.billing_address_2
        ,orders.billing_city
        ,orders.billing_state
        ,orders.billing_postal_code
        ,order_shipment.shipment_postage_carrier
        ,coalesce(order_revenue.gross_product_revenue) as gross_product_revenue
        ,coalesce(order_revenue.membership_discount) as membership_discount
        ,coalesce(order_revenue.merch_discount) as merch_discount
        ,coalesce(order_revenue.moolah_item_discount) as moolah_item_discount
        ,coalesce(order_revenue.moolah_order_discount) as moolah_order_discount
        ,coalesce(order_revenue.free_protein_promotion) as free_protein_promotion
        ,coalesce(order_revenue.item_promotion) as item_promotion
        ,coalesce(order_revenue.net_product_revenue) as net_product_revenue
        ,orders.order_shipping_fee_usd as shipping_revenue
        ,orders.order_expedited_shipping_fee_usd as expedited_shipping_revenue
        ,coalesce(order_revenue.free_shipping_discount) as free_shipping_discount
        ,coalesce(order_revenue.gross_revenue) as gross_revenue
        ,coalesce(order_revenue.new_member_discount) as new_member_discount
        ,coalesce(order_revenue.refund_amount) as refund_amount
        ,coalesce(order_revenue.gift_redemption) as gift_redemption
        ,coalesce(order_revenue.other_discount) as other_discount
        ,coalesce(order_revenue.net_revenue) as net_revenue
        ,coalesce(order_cost.product_cost) as product_cost
        ,coalesce(order_cost.shipment_cost) as shipment_cost
        ,coalesce(order_cost.order_coolant_cost) as coolant_cost
        ,coalesce(order_cost.order_packaging_cost) as packaging_cost
        ,coalesce(order_cost.order_care_cost) as care_cost
        ,coalesce(order_cost.order_picking_cost) as picking_cost
        ,coalesce(order_cost.order_packing_cost) as packing_cost
        ,coalesce(order_cost.order_box_making_cost) as box_making_cost
        ,coalesce(order_cost.order_fc_other_cost) as fc_other_cost
        ,coalesce(order_cost.order_fc_labor_cost) as fc_labor_cost
        ,coalesce(order_cost.poseidon_fulfillment_cost) as poseidon_fulfillment_cost
        ,coalesce(order_cost.inbound_shipping_cost) as inbound_shipping_cost
        ,if(orders.stripe_charge_id is not null,order_revenue.net_revenue * 0.0274,0) as payment_processing_cost
        ,orders.coolant_weight_in_pounds
        ,orders.order_additional_coolant_weight_in_pounds
        ,orders.order_bids_count
        ,coalesce(order_shipment.shipment_count) as shipment_count
        ,order_shipment.delivery_days_late
        ,order_shipment.shipment_tracking_code_list
        ,coalesce(order_reschedule.reschedule_count) as reschedule_count
        ,orders.is_rastellis
        ,orders.is_qvc
        ,orders.is_seabear
        ,orders.is_backyard_butchers
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
        ,flags.is_moolah_order
        ,flags.has_shipped
        ,flags.has_been_delivered
        ,flags.has_been_lost
        ,flags.is_fulfillment_risk
        ,flags.is_rescheduled
        ,flags.was_member
        ,flags.was_week_out_notification_sent
        ,flags.is_all_inventory_reserved
        ,flags.does_need_customer_confirmation
        ,flags.is_time_to_charge
        ,flags.is_payment_failure
        ,flags.was_referred_to_customer_service
        ,flags.is_invalid_postal_code
        ,flags.can_retry_payment
        ,flags.is_under_order_minimum
        ,flags.is_order_scheduled_in_past
        ,flags.is_order_missing
        ,flags.is_order_cancelled
        ,flags.is_order_charged
        ,flags.is_bids_fulfillment_at_risk
        ,flags.has_gift_card_redemption
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
        ,ranks.moolah_order_rank
        ,ranks.completed_moolah_order_rank
        ,ranks.paid_moolah_order_rank
        ,ranks.cancelled_moolah_order_rank
        ,coalesce(units.beef_units) as beef_units
        ,coalesce(units.bison_units) as bison_units
        ,coalesce(units.chicken_units) as chicken_units
        ,coalesce(units.desserts_units) as desserts_units 
        ,coalesce(units.duck_units) as duck_units
        ,coalesce(units.game_meat_units) as game_meat_units
        ,coalesce(units.japanese_wagyu_units) as japanese_wagyu_units
        ,coalesce(units.lamb_units) as lamb_units
        ,coalesce(units.pet_food_units) as pet_food_units
        ,coalesce(units.plant_based_proteins_units) as plant_based_proteins_units
        ,coalesce(units.pork_units) as pork_units
        ,coalesce(units.salts_seasonings_units) as salts_seasonings_units
        ,coalesce(units.seafood_units) as seafood_units
        ,coalesce(units.starters_sides_units) as starters_sides_units
        ,coalesce(units.turkey_units) as turkey_units
        ,coalesce(units.wagyu_units) as wagyu_units
        ,coalesce(units.bundle_units) as bundle_units
        ,coalesce(units.total_units) as total_units
        ,if(units.blackwing_turkey_units>0,TRUE,FALSE) as contains_turkey
        ,coalesce(units.total_product_weight) as total_product_weight
        ,coalesce(units.pct_beef) as pct_beef
        ,coalesce(units.pct_bison) as pct_bison
        ,coalesce(units.pct_chicken) as pct_chicken
        ,coalesce(units.pct_desserts) as pct_desserts
        ,coalesce(units.pct_duck) as pct_duck
        ,coalesce(units.pct_game_meat) as pct_game_meat
        ,coalesce(units.pct_japanese_wagyu) as pct_japanese_wagyu
        ,coalesce(units.pct_lamb) as pct_lamb
        ,coalesce(units.pct_pet_food) as pct_pet_food
        ,coalesce(units.pct_plant_based_proteins) as pct_plant_based_proteins
        ,coalesce(units.pct_pork) as pct_pork
        ,coalesce(units.pct_salts_seasonings) as pct_salts_seasonings 
        ,coalesce(units.pct_seafood) as pct_seafood
        ,coalesce(units.pct_starters_sides) as pct_starters_sides
        ,coalesce(units.pct_turkey) as pct_turkey
        ,coalesce(units.pct_wagyu) as pct_wagyu
        ,coalesce(units.pct_bundle) as pct_bundle
        ,coalesce(reward.jwagyu_reward_spend) as jwagyu_reward_spend
        ,coalesce(reward.moolah_points) as total_moolah_balance_change
        ,coalesce(reward.total_moolah_redeemed) as total_moolah_redeemed
        ,coalesce(reward.total_awarded_moolah) as total_awarded_moolah
        ,coalesce(reward.moolah_available_for_order) as moolah_available_for_order

        
        ,IF(
        units.beef_units > 0,
        SUM(CASE WHEN units.beef_units > 0 THEN 1 ELSE 0 END) OVER (PARTITION BY orders.user_id ORDER BY orders.order_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        NULL
        ) AS beef_item_rank

        ,IF(
        units.bison_units > 0,
        SUM(CASE WHEN units.bison_units > 0 THEN 1 ELSE 0 END) OVER (PARTITION BY orders.user_id ORDER BY orders.order_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        NULL
        ) AS bison_item_rank

        ,IF(
        units.chicken_units > 0,
        SUM(CASE WHEN units.chicken_units > 0 THEN 1 ELSE 0 END) OVER (PARTITION BY orders.user_id ORDER BY orders.order_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        NULL
        ) AS chicken_item_rank

        ,IF(
        units.desserts_units > 0,
        SUM(CASE WHEN units.desserts_units > 0 THEN 1 ELSE 0 END) OVER (PARTITION BY orders.user_id ORDER BY orders.order_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        NULL
        ) AS desserts_item_rank

        ,IF(
        units.japanese_wagyu_units > 0,
        SUM(CASE WHEN units.japanese_wagyu_units > 0 THEN 1 ELSE 0 END) OVER (PARTITION BY orders.user_id ORDER BY orders.order_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        NULL
        ) AS japanese_wagyu_item_rank

        ,IF(
        units.lamb_units > 0,
        SUM(CASE WHEN units.lamb_units > 0 THEN 1 ELSE 0 END) OVER (PARTITION BY orders.user_id ORDER BY orders.order_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        NULL
        ) AS lamb_item_rank

        ,IF(
        units.pork_units > 0,
        SUM(CASE WHEN units.pork_units > 0 THEN 1 ELSE 0 END) OVER (PARTITION BY orders.user_id ORDER BY orders.order_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        NULL
        ) AS pork_item_rank

        ,IF(
        units.seafood_units > 0,
        SUM(CASE WHEN units.seafood_units > 0 THEN 1 ELSE 0 END) OVER (PARTITION BY orders.user_id ORDER BY orders.order_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        NULL
        ) AS seafood_item_rank

        ,IF(
        units.starters_sides_units > 0,
        SUM(CASE WHEN units.starters_sides_units > 0 THEN 1 ELSE 0 END) OVER (PARTITION BY orders.user_id ORDER BY orders.order_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        NULL
        ) AS starters_sides_item_rank

        ,IF(
        units.turkey_units > 0,
        SUM(CASE WHEN units.turkey_units > 0 THEN 1 ELSE 0 END) OVER (PARTITION BY orders.user_id ORDER BY orders.order_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        NULL
        ) AS turkey_item_rank

        ,IF(
        units.wagyu_units > 0,
        SUM(CASE WHEN units.wagyu_units > 0 THEN 1 ELSE 0 END) OVER (PARTITION BY orders.user_id ORDER BY orders.order_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        NULL
        ) AS wagyu_item_rank

        ,IF(
        units.bundle_units > 0,
        SUM(CASE WHEN units.bundle_units > 0 THEN 1 ELSE 0 END) OVER (PARTITION BY orders.user_id ORDER BY orders.order_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        NULL
        ) AS bundle_item_rank

        ,orders.order_created_at_utc
        ,orders.order_updated_at_utc
        ,orders.order_checkout_completed_at_utc
        ,orders.order_cancelled_at_utc
        ,orders.order_paid_at_utc
        ,orders.order_first_stuck_at_utc
        ,orders.customer_viewed_at_utc
        ,orders.next_box_notified_at_utc
        ,orders.order_scheduled_fulfillment_date_utc
        ,orders.order_scheduled_arrival_date_utc
        ,orders.gel_pack_count
        ,orders.is_recurring
        ,order_shipment.shipped_at_utc
        ,order_shipment.delivered_at_utc
        ,order_shipment.lost_at_utc
        ,order_shipment.original_est_delivery_date_utc
        ,order_shipment.est_delivery_date_utc
        ,order_reschedule.occurred_at_utc as order_reschedule_occurred_at_utc
        ,order_reschedule.old_scheduled_fulfillment_date
        ,order_reschedule.new_scheduled_fulfillment_date
        ,if(order_reschedule.is_customer_reschedule and old_scheduled_fulfillment_date < new_scheduled_fulfillment_date and date_diff(order_reschedule.new_scheduled_fulfillment_date,order_reschedule.old_scheduled_fulfillment_date, day) >= 14, true,false) as is_customer_impactful_reschedule
        ,order_promo_redeemed.redeemed_at_utc as promo_redeemed_at_utc
        ,order_shipment_option.transit_days as plan_transit_days
        
    from orders
        left join order_revenue on orders.order_id = order_revenue.order_id
        left join order_cost on orders.order_id = order_cost.order_id
        left join flags on orders.order_id = flags.order_id
        left join ranks on orders.order_id = ranks.order_id
        left join units on orders.order_id = units.order_id
        left join order_shipment on orders.order_id = order_shipment.order_id
        left join order_reschedule on cast(orders.order_id as string) = order_reschedule.order_id
        left join order_promo_redeemed on orders.order_id = order_promo_redeemed.order_id
        left join order_failure on orders.order_id = order_failure.order_id
        left join reward on orders.order_id = reward.order_id
        left join payment_methods on orders.order_id = payment_methods.order_id
        left join order_shipment_option on orders.order_id = order_shipment_option.order_id 
    
    /**** Removing these order types because they are just shell orders that provide no data value ****/
    /**** Children orders contain all the necessary information for revenue, addresses, dates, etc for the order ****/
    where orders.order_type not in ('BULK ORDER','BULK GIFT')
)
,membership_status_at_order as (
    select  distinct
        order_joins.order_id
        ,if(user_membership.user_id is null and order_joins.subscription_id is null, false, true) as uncancelled_member_at_order_time
    from order_joins
    left join user_membership on order_joins.user_id = user_membership.user_id and user_membership.subscription_created_at_utc < order_joins.order_paid_at_utc and (user_membership.subscription_cancelled_at_utc is null or user_membership.subscription_cancelled_at_utc > order_joins.order_paid_at_utc) and order_joins.subscription_id is null
)

,calc_margin as (
    select
        order_joins.*
        ,net_product_revenue - product_cost as product_profit
        ,net_revenue - product_cost - shipment_cost - packaging_cost - payment_processing_cost
            - coolant_cost - care_cost - fc_labor_cost - poseidon_fulfillment_cost - inbound_shipping_cost as gross_profit
        ,membership_status_at_order.uncancelled_member_at_order_time
    from order_joins
    left join membership_status_at_order on membership_status_at_order.order_id = order_joins.order_id
)
,order_bucket as (
    select 
    calc_margin.*
    ,case
         when not paid_order_rank = 1 and not is_ala_carte_order and not is_gift_card_order and not is_bulk_gift_order and not is_gift_order and not contains_turkey then 'Subscription Orders'
         when not paid_order_rank = 1 and not is_ala_carte_order and not is_gift_card_order and not is_bulk_gift_order and not is_gift_order and contains_turkey then 'Subscription Orders With BW Turkey'
         when not paid_order_rank = 1 and is_ala_carte_order and not is_gift_card_order and not is_bulk_gift_order and not is_gift_order and contains_turkey and uncancelled_member_at_order_time then 'Subscription Customer | ALC Orders With BW Turkey'
         when not paid_order_rank = 1 and is_ala_carte_order and not is_gift_card_order and not is_bulk_gift_order and is_gift_order and not contains_turkey and uncancelled_member_at_order_time then 'Subscription Customer | ALC Consumer Gift Order'
         when not paid_order_rank = 1 and not is_ala_carte_order and not is_gift_card_order and is_bulk_gift_order and not is_gift_order and not contains_turkey then 'Corp Gift Order'
         when not paid_order_rank = 1 and is_ala_carte_order and not is_gift_card_order and not is_bulk_gift_order and is_gift_order and contains_turkey and uncancelled_member_at_order_time then 'Subscription Customer | ALC Consumer Gift Order With BW Turkey'
         when not paid_order_rank = 1 and is_ala_carte_order and is_gift_card_order and not is_bulk_gift_order and not is_gift_order and not contains_turkey and uncancelled_member_at_order_time then 'Subscription Customer | ALC Gift Card Order'
         when not paid_order_rank = 1 and is_ala_carte_order and not is_gift_card_order and not is_bulk_gift_order and not is_gift_order and not contains_turkey and uncancelled_member_at_order_time then 'Subscription Customer | ALC Orders'
         when paid_order_rank = 1 and not is_ala_carte_order and not is_gift_card_order and not is_bulk_gift_order and not is_gift_order and not contains_turkey then 'New Subscription Orders'
         when paid_order_rank = 1 and not is_ala_carte_order and not is_gift_card_order and not is_bulk_gift_order and not is_gift_order and contains_turkey then 'New Subscription Orders With BW Turkey'
         when not paid_order_rank = 1 and is_ala_carte_order and not is_gift_card_order and not is_bulk_gift_order and not is_gift_order and not contains_turkey then 'ALC Orders'
         when not paid_order_rank = 1 and is_ala_carte_order and not is_gift_card_order and not is_bulk_gift_order and not is_gift_order and contains_turkey and not uncancelled_member_at_order_time then 'ALC Orders With BW Turkey'
         when not paid_order_rank = 1 and is_ala_carte_order and not is_gift_card_order and not is_bulk_gift_order and is_gift_order and not contains_turkey and not uncancelled_member_at_order_time then 'ALC Consumer Gift Order'
         when not paid_order_rank = 1 and is_ala_carte_order and not is_gift_card_order and not is_bulk_gift_order and is_gift_order and contains_turkey and not uncancelled_member_at_order_time then 'ALC Consumer Gift Order With BW Turkey'
         when not paid_order_rank = 1 and is_ala_carte_order and is_gift_card_order and not is_bulk_gift_order and not is_gift_order and not contains_turkey and not uncancelled_member_at_order_time then 'ALC Gift Card Order'
         when paid_order_rank = 1 and is_ala_carte_order and not is_gift_card_order and not is_bulk_gift_order and not is_gift_order and not contains_turkey then 'New ALC Orders'
         when paid_order_rank = 1 and is_ala_carte_order and not is_gift_card_order and not is_bulk_gift_order and not is_gift_order and contains_turkey then 'New ALC Orders With BW Turkey'
         when paid_order_rank = 1 and is_ala_carte_order and not is_gift_card_order and not is_bulk_gift_order and is_gift_order and not contains_turkey and not uncancelled_member_at_order_time then 'New ALC Consumer Gift Order'
         when paid_order_rank = 1 and is_ala_carte_order and not is_gift_card_order and not is_bulk_gift_order and is_gift_order and contains_turkey and not uncancelled_member_at_order_time then 'New ALC Consumer Gift Order With BW Turkey'
         when paid_order_rank = 1 and is_ala_carte_order and is_gift_card_order and not is_bulk_gift_order and not is_gift_order and not contains_turkey and not uncancelled_member_at_order_time then 'New ALC Gift Card Order'
    else null end as order_bucket
    from calc_margin
)

select * from order_bucket
