with

credit as ( select * from {{ ref('stg_cc__credits') }} )
,awarded_cow_cash as ( select * from {{ ref('stg_cc__cow_cash_entries') }} )
,promotion as ( select * from {{ ref('stg_cc__promotions') }} )

,add_cow_cash_information as (
    select
        credit.credit_id
        ,credit.promotion_id
        ,credit.user_id
        ,credit.order_id
        ,credit.cow_cash_entry_source_id
        ,credit.credit_type
        ,awarded_cow_cash.entry_type as awarded_cow_cash_entry_type
        ,credit.credit_description
        ,awarded_cow_cash.cow_cash_message as awarded_cow_cash_message
        ,credit.credit_discount_usd
        ,credit.discount_percent
        ,credit.is_hidden_from_user
        ,credit.is_controlled_by_promotion
        ,credit.created_at_utc
        ,credit.updated_at_utc
    from credit
        left join awarded_cow_cash on credit.cow_cash_entry_source_id = awarded_cow_cash.cow_cash_id
)

,add_promotion_info as (
    select 
        add_cow_cash_information.credit_id
        ,add_cow_cash_information.promotion_id
        ,add_cow_cash_information.user_id
        ,add_cow_cash_information.order_id
        ,add_cow_cash_information.cow_cash_entry_source_id
        ,add_cow_cash_information.credit_type
        ,promotion.promotion_type
        ,add_cow_cash_information.awarded_cow_cash_entry_type
        ,add_cow_cash_information.credit_description
        ,add_cow_cash_information.awarded_cow_cash_message
        ,add_cow_cash_information.credit_discount_usd
        ,add_cow_cash_information.discount_percent
        ,add_cow_cash_information.is_hidden_from_user
        ,add_cow_cash_information.is_controlled_by_promotion
        ,coalesce(promotion.promotion_type in ('FREE_GROUND_BEEF','FREE_GROUND_WAGYU','FREE_SAUSAGE','FREE_PATTIES','FREE_SUMMER_SEAFOOD_BOX','NEW_SUBSCRIPTION_FREE_SPARERIBS','NEW_SUBSCRIPTION_25_PERCENT'
                         ,'FREE_SALMON','SUBSCRIPTION_LIFETIME_GROUND_BEEF','SUBSCRIPTION_LIFETIME_GROUND_WAGYU','FREE_STEAK','SUBSCRIPTION_LIFETIME_BACON','SUBSCRIPTION_100_OFF_3_ORDERS'
                         ,'SUBSCRIPTION_100_OFF_34_33_33','SUBSCRIPTION_100_OFF_10_X_10'),FALSE) as is_new_member_promotion
        ,add_cow_cash_information.created_at_utc
        ,add_cow_cash_information.updated_at_utc
    from add_cow_cash_information
        left join promotion on add_cow_cash_information.promotion_id = promotion.promotion_id
)

,group_credits as (
    select
        credit_id
        ,promotion_id
        ,user_id
        ,order_id
        ,cow_cash_entry_source_id
        ,credit_type
        ,credit_description

        ,case
            when credit_type = 'FREE_SHIPPING' then 'Free Shipping'
            when credit_type = 'SUBSCRIPTION_FIVE_PERCENT'
                or (credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'SUBSCRIPTION') then 'Membership 5%'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'CUSTOMER_SERVICE' then 'CARE Credits'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'REFERRAL' then 'Acquisition Marketing - Member Referral'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'BULK_ORDER' then 'Corporate Gifting'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'GIFT_CARD' then 'Gift Card Redemption'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'PROMOTION' then 'Acquisition Marketing - Promotion Credits'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'GIFT_CARD_PROMOTION' then 'Acquisition Marketing - Gift'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'RETENTION_OFFER' then 'Retention Marketing'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'REFERRED_CREDIT' then 'Acquisition Marketing - Member Referral'
            when credit_type = 'DOLLAR_AMOUNT' and promotion_id in (28,29,30) then 'Acquisition Marketing - Promotion Credits'
            when credit_type = 'DOLLAR_AMOUNT' and promotion_id is null then 'Various -- ?'
            when credit_type in ('GIFT_CODE_DOLLAR_AMOUNT','PERCENT_DISCOUNT') and promotion_id = 7 then 'Acquisition Marketing - Gift'
            when credit_type = 'GIFT_CODE_DOLLAR_AMOUNT' and promotion_id = 10 then 'Acquisition Marketing - Member Referral'
            when credit_type = 'GIFT_CODE_DOLLAR_AMOUNT' and promotion_id = 8 then 'Acquisition Marketing - Influencer'
            else 'Other - UNKNOWN'
        end as credit_business_group

        ,case
            when credit_type = 'FREE_SHIPPING' then '41305 - Free Shipping'
            when credit_type = 'SUBSCRIPTION_FIVE_PERCENT'
                or (credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'SUBSCRIPTION') then '41303 - Subscription Rewards'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'CUSTOMER_SERVICE' then '41307 - Care Concessions'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'REFERRAL' then '61145 - Referral Credits'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'BULK_ORDER' then 'Corporate Gifting'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'GIFT_CARD' then 'Gift Card Redemption'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'PROMOTION' then '41301 - New Customer Subscriptions'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'GIFT_CARD_PROMOTION' then '41306 - Other'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'RETENTION_OFFER' then '41306 - Other'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'REFERRED_CREDIT' then '61145 - Referral Credits'
            when credit_type = 'DOLLAR_AMOUNT' and promotion_id in (28,29,30) then '41301 - New Customer Subscriptions'
            when credit_type = 'DOLLAR_AMOUNT' and promotion_id is null then 'Various -- ?'
            when credit_type in ('GIFT_CODE_DOLLAR_AMOUNT','PERCENT_DISCOUNT') and promotion_id = 7 then '41306 - Other'
            when credit_type = 'GIFT_CODE_DOLLAR_AMOUNT' and promotion_id = 10 then '61145 - Referral Credits'
            when credit_type = 'GIFT_CODE_DOLLAR_AMOUNT' and promotion_id = 8 then '41301 - New Customer Subscriptions'
            else 'Other - UNKNOWN'
        end as credit_financial_account

        ,awarded_cow_cash_entry_type
        ,awarded_cow_cash_message
        ,promotion_type
        ,credit_discount_usd
        ,discount_percent
        ,is_hidden_from_user
        ,is_controlled_by_promotion
        ,is_new_member_promotion
        ,created_at_utc
        ,updated_at_utc
    from add_promotion_info
)

select * from group_credits
