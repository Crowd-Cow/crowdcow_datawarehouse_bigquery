with

promotion as ( select * from {{ ref('stg_cc__promotions') }} where dbt_valid_to is null )


,add_new_member_flag as (
    select
        promotion_id
        ,promotion_type
        ,promotion_source
        ,promo_code
        ,promotion_key_value

        ,coalesce(promotion.promotion_type in 
            ('FREE_GROUND_BEEF','FREE_GROUND_WAGYU','FREE_SAUSAGE','FREE_PATTIES','FREE_SUMMER_SEAFOOD_BOX','NEW_SUBSCRIPTION_FREE_SPARERIBS','NEW_SUBSCRIPTION_25_PERCENT'
                ,'FREE_SALMON','SUBSCRIPTION_LIFETIME_GROUND_BEEF','SUBSCRIPTION_LIFETIME_GROUND_WAGYU','FREE_STEAK','SUBSCRIPTION_LIFETIME_BACON','SUBSCRIPTION_100_OFF_3_ORDERS'
                ,'SUBSCRIPTION_100_OFF_34_33_33','SUBSCRIPTION_100_OFF_10_X_10'),FALSE) as is_new_member_promotion  
                                       
        ,is_always_available
        ,must_be_assigned_to_user
        ,must_be_assigned_to_order
        ,claimable_window_in_days
        ,must_be_claimed
        ,must_be_applied_by_user
        ,starts_at_utc
        ,ends_at_utc
        ,created_at_utc
        ,updated_at_utc
    from promotion
)

select * from add_new_member_flag
