with

orders as ( select * from {{ ref('stg_cc__orders') }} )
,order_revenue as ( select * from {{ ref('int_order_revenue') }} )
,order_cost as ( select * from {{ ref('int_order_costs') }} )
,flags as ( select * from {{ ref('int_order_flags') }} )
,ranks as ( select * from {{ ref('int_order_ranks') }} )
,units as ( select * from {{ ref('int_order_units_pct') }} )
,order_shipment as ( select * from {{ ref('int_order_shipments') }} )
,order_reschedule as ( select * from {{ ref('int_order_reschedules') }} )

,order_joins as (
    select
        orders.order_id
        ,orders.parent_order_id
        ,orders.order_token
        ,orders.user_id
        ,orders.subscription_id
        ,orders.fc_id
        ,orders.visit_id
        ,orders.stripe_charge_id
        ,{{ get_join_key('stg_cc__fcs','fc_key','fc_id','orders','fc_id','order_updated_at_utc') }} as fc_key
        ,orders.order_identifier
        ,orders.order_current_state
        ,order_reschedule.reschedule_reason

        ,case
            when orders.parent_order_id is not null then 'CORP GIFT'
            else orders.order_type
         end as order_type
        
        ,orders.stripe_failure_code
        ,orders.order_delivery_street_address_1
        ,orders.order_delivery_street_address_2
        ,orders.order_delivery_city
        ,orders.order_delivery_state
        ,orders.order_delivery_postal_code
        ,orders.order_delivery_county_name
        ,orders.billing_address_1
        ,orders.billing_address_2
        ,orders.billing_city
        ,orders.billing_state
        ,orders.billing_postal_code
        ,order_shipment.shipment_postage_carrier
        ,zeroifnull(order_revenue.gross_product_revenue) as gross_product_revenue
        ,zeroifnull(order_revenue.membership_discount) as membership_discount
        ,zeroifnull(order_revenue.merch_discount) as merch_discount
        ,zeroifnull(order_revenue.free_protein_promotion) as free_protein_promotion
        ,zeroifnull(order_revenue.net_product_revenue) as net_product_revenue
        ,orders.order_shipping_fee_usd as shipping_revenue
        ,zeroifnull(order_revenue.free_shipping_discount) as free_shipping_discount
        ,zeroifnull(order_revenue.gross_revenue) as gross_revenue
        ,zeroifnull(order_revenue.new_member_discount) as new_member_discount
        ,zeroifnull(order_revenue.refund_amount) as refund_amount
        ,zeroifnull(order_revenue.gift_redemption) as gift_redemption
        ,zeroifnull(order_revenue.other_discount) as other_discount
        ,zeroifnull(order_revenue.net_revenue) as net_revenue
        ,zeroifnull(order_cost.product_cost) as product_cost
        ,zeroifnull(order_cost.shipment_cost) as shipment_cost
        ,zeroifnull(order_cost.order_coolant_cost) as coolant_cost
        ,zeroifnull(order_cost.order_packaging_cost) as packaging_cost
        ,zeroifnull(order_cost.order_care_cost) as care_cost
        ,zeroifnull(order_cost.order_picking_cost) as picking_cost
        ,zeroifnull(order_cost.order_packing_cost) as packing_cost
        ,zeroifnull(order_cost.order_box_making_cost) as box_making_cost
        ,zeroifnull(order_cost.order_fc_other_cost) as fc_other_cost
        ,zeroifnull(order_cost.order_fc_labor_cost) as fc_labor_cost
        ,zeroifnull(order_cost.poseidon_fulfillment_cost) as poseidon_fulfillment_cost
        ,zeroifnull(order_cost.inbound_shipping_cost) as inbound_shipping_cost
        ,iff(orders.stripe_charge_id is not null,order_revenue.net_revenue * 0.0274,0) as payment_processing_cost
        ,orders.coolant_weight_in_pounds
        ,orders.order_additional_coolant_weight_in_pounds
        ,orders.order_bids_count
        ,zeroifnull(order_shipment.shipment_count) as shipment_count
        ,zeroifnull(order_reschedule.reschedule_count) as reschedule_count
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
        ,flags.has_shipped
        ,flags.has_been_delivered
        ,flags.has_been_lost
        ,flags.is_fulfillment_risk
        ,flags.is_rescheduled
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
        ,zeroifnull(units.beef_units) as beef_units
        ,zeroifnull(units.bison_units) as bison_units
        ,zeroifnull(units.chicken_units) as chicken_units
        ,zeroifnull(units.desserts_units) as desserts_units 
        ,zeroifnull(units.duck_units) as duck_units
        ,zeroifnull(units.game_meat_units) as game_meat_units
        ,zeroifnull(units.japanese_wagyu_units) as japanese_wagyu_units
        ,zeroifnull(units.lamb_units) as lamb_units
        ,zeroifnull(units.pet_food_units) as pet_food_units
        ,zeroifnull(units.plant_based_proteins_units) as plant_based_proteins_units
        ,zeroifnull(units.pork_units) as pork_units
        ,zeroifnull(units.salts_seasonings_units) as salts_seasonings_units
        ,zeroifnull(units.seafood_units) as seafood_units
        ,zeroifnull(units.starters_sides_units) as starters_sides_units
        ,zeroifnull(units.turkey_units) as turkey_units
        ,zeroifnull(units.wagyu_units) as wagyu_units
        ,zeroifnull(units.bundle_units) as bundle_units
        ,zeroifnull(units.total_units) as total_units
        ,zeroifnull(units.total_product_weight) as total_product_weight
        ,zeroifnull(units.pct_beef) as pct_beef
        ,zeroifnull(units.pct_bison) as pct_bison
        ,zeroifnull(units.pct_chicken) as pct_chicken
        ,zeroifnull(units.pct_desserts) as pct_desserts
        ,zeroifnull(units.pct_duck) as pct_duck
        ,zeroifnull(units.pct_game_meat) as pct_game_meat
        ,zeroifnull(units.pct_japanese_wagyu) as pct_japanese_wagyu
        ,zeroifnull(units.pct_lamb) as pct_lamb
        ,zeroifnull(units.pct_pet_food) as pct_pet_food
        ,zeroifnull(units.pct_plant_based_proteins) as pct_plant_based_proteins
        ,zeroifnull(units.pct_pork) as pct_pork
        ,zeroifnull(units.pct_salts_seasonings) as pct_salts_seasonings 
        ,zeroifnull(units.pct_seafood) as pct_seafood
        ,zeroifnull(units.pct_starters_sides) as pct_starters_sides
        ,zeroifnull(units.pct_turkey) as pct_turkey
        ,zeroifnull(units.pct_wagyu) as pct_wagyu
        ,zeroifnull(units.pct_bundle) as pct_bundle
        
        ,iff(
            units.beef_units > 0
            ,conditional_true_event(units.beef_units > 0) over(partition by orders.user_id order by orders.order_id)
            ,null
        ) as beef_item_rank
        ,iff(
            units.bison_units > 0
            ,conditional_true_event(units.bison_units > 0) over(partition by orders.user_id order by orders.order_id)
            ,null
        ) as bison_item_rank
        ,iff(
            units.chicken_units > 0
            ,conditional_true_event(units.chicken_units > 0) over(partition by orders.user_id order by orders.order_id)
            ,null
        ) as chicken_item_rank
        ,iff(
            units.desserts_units > 0
            ,conditional_true_event(units.desserts_units > 0) over(partition by orders.user_id order by orders.order_id)
            ,null
        ) as desserts_item_rank
        ,iff(
            units.japanese_wagyu_units > 0
            ,conditional_true_event(units.japanese_wagyu_units > 0) over(partition by orders.user_id order by orders.order_id)
            ,null
        ) as japanese_wagyu_item_rank
        ,iff(
            units.lamb_units > 0
            ,conditional_true_event(units.lamb_units > 0) over(partition by orders.user_id order by orders.order_id)
            ,null
        ) as lamb_item_rank
        ,iff(
            units.pork_units > 0
            ,conditional_true_event(units.pork_units > 0) over(partition by orders.user_id order by orders.order_id)
            ,null
        ) as pork_item_rank
        ,iff(
            units.seafood_units > 0
            ,conditional_true_event(units.seafood_units > 0) over(partition by orders.user_id order by orders.order_id)
            ,null
        ) as seafood_item_rank
        ,iff(
            units.starters_sides_units > 0
            ,conditional_true_event(units.starters_sides_units > 0) over(partition by orders.user_id order by orders.order_id)
            ,null
        ) as starters_sides_item_rank
        ,iff(
            units.turkey_units > 0
            ,conditional_true_event(units.turkey_units > 0) over(partition by orders.user_id order by orders.order_id)
            ,null
        ) as turkey_item_rank
        ,iff(
            units.wagyu_units > 0
            ,conditional_true_event(units.wagyu_units > 0) over(partition by orders.user_id order by orders.order_id)
            ,null
        ) as wagyu_item_rank
        ,iff(
            units.bundle_units > 0
            ,conditional_true_event(units.bundle_units > 0) over(partition by orders.user_id order by orders.order_id)
            ,null
        ) as bundle_item_rank

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
        ,order_shipment.shipped_at_utc
        ,order_shipment.delivered_at_utc
        ,order_shipment.lost_at_utc
        ,order_reschedule.occurred_at_utc as order_reschedule_occurred_at_utc
        ,order_reschedule.old_scheduled_fulfillment_date
        ,order_reschedule.new_scheduled_fulfillment_date
        
    from orders
        left join order_revenue on orders.order_id = order_revenue.order_id
        left join order_cost on orders.order_id = order_cost.order_id
        left join flags on orders.order_id = flags.order_id
        left join ranks on orders.order_id = ranks.order_id
        left join units on orders.order_id = units.order_id
        left join order_shipment on orders.order_id = order_shipment.order_id
        left join order_reschedule on orders.order_id = order_reschedule.order_id
    
    /**** Removing these order types because they are just shell orders that provide no data value ****/
    /**** Children orders contain all the necessary information for revenue, addresses, dates, etc for the order ****/
    where orders.order_type not in ('BULK ORDER','BULK GIFT')
)

select * from order_joins
