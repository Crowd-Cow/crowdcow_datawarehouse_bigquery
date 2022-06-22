with

credit as ( select * from {{ ref('stg_cc__credits') }} )
,awarded_cow_cash as ( select * from {{ ref('stg_cc__cow_cash_entries') }} )
,promotion as ( select * from {{ ref('promotions') }} )
,orders as ( select * from {{ ref('stg_cc__orders') }} )

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
        ,credit.promotion_source
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
        ,add_cow_cash_information.promotion_source
        ,add_cow_cash_information.awarded_cow_cash_entry_type
        ,add_cow_cash_information.credit_description
        ,add_cow_cash_information.awarded_cow_cash_message
        ,add_cow_cash_information.credit_discount_usd
        ,add_cow_cash_information.discount_percent
        ,add_cow_cash_information.is_hidden_from_user
        ,add_cow_cash_information.is_controlled_by_promotion
        ,promotion.is_new_member_promotion
        ,add_cow_cash_information.created_at_utc
        ,add_cow_cash_information.updated_at_utc
    from add_cow_cash_information
        left join promotion on add_cow_cash_information.promotion_id = promotion.promotion_id
            and add_cow_cash_information.promotion_source = promotion.promotion_source
)

,add_order_type as (
    select
        add_promotion_info.*
        ,orders.order_type
    from add_promotion_info
        left join orders on add_promotion_info.order_id = orders.order_id
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
        ,promotion_source

        ,case
            when credit_type = 'FREE_SHIPPING' then 'FREE SHIPPING'
            when credit_type = 'SUBSCRIPTION_FIVE_PERCENT'
                or (credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'SUBSCRIPTION') then 'MEMBERSHIP 5%'
            when (credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'CUSTOMER_SERVICE') 
                or (credit_type = 'DOLLAR_AMOUNT' and promotion_id is null and order_type in ('AMAZON','E-COMMERCE')) then 'CARE CREDITS'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'REFERRAL' then 'ACQUISITION MARKETING - MEMBER REFERRAL'
            when (credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'BULK_ORDER') 
                or (credit_type = 'DOLLAR_AMOUNT' and promotion_id is null and order_type = 'BULK ORDER') then 'CORPORATE GIFTING'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'GIFT_CARD' then 'GIFT CARD REDEMPTION'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'PROMOTION' then 'ACQUISITION MARKETING - PROMOTION CREDITS'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'GIFT_CARD_PROMOTION' then 'ACQUISITION MARKETING - GIFT'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'RETENTION_OFFER' then 'RETENTION MARKETING'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'REFERRED_CREDIT' then 'ACQUISITION MARKETING - MEMBER REFERRAL'
            when credit_type = 'DOLLAR_AMOUNT' and promotion_id in (28,29,30) and promotion_source = 'PROMOTION' then 'ACQUISITION MARKETING - PROMOTION CREDITS'
            when credit_type = 'DOLLAR_AMOUNT' and promotion_id is null and order_type = 'REPLACEMENT' then 'REPLACEMENTS'
            when credit_type = 'DOLLAR_AMOUNT' and promotion_id is null 
                and order_type in ('MARKETING EVENTS','MARKETING INFLUENCER','PHOTO SHOOTS','PR SAMPLES','WHOLESALE','WHOLESALE SAMPLES') then 'MARKETING PR'
            when credit_type = 'DOLLAR_AMOUNT' and promotion_id is null and order_type = 'GIFT' then 'INTERNAL - HR RELATED'
            when credit_type in ('GIFT_CODE_DOLLAR_AMOUNT','PERCENT_DISCOUNT') and promotion_id = 7 and promotion_source = 'PROMOTION' then 'ACQUISITION MARKETING - GIFT'
            when credit_type = 'GIFT_CODE_DOLLAR_AMOUNT' and promotion_id = 10 and promotion_source = 'PROMOTION' then 'ACQUISITION MARKETING - MEMBER REFERRAL'
            when credit_type = 'GIFT_CODE_DOLLAR_AMOUNT' and promotion_id = 8 and promotion_source = 'PROMOTION' then 'ACQUISITION MARKETING - INFLUENCER'
            when credit_type = 'DOLLAR_AMOUNT' and promotion_id = 2 and promotion_source = 'PROMOTIONS::PROMOTION' then 'MERCHANDISING DISCOUNT'
            else 'OTHER - UNKNOWN'
        end as credit_business_group

        ,case
            when credit_type = 'FREE_SHIPPING' then '41305 - FREE SHIPPING'
            when credit_type = 'SUBSCRIPTION_FIVE_PERCENT'
                or (credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'SUBSCRIPTION') then '41303 - SUBSCRIPTION REWARDS'
            when (credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'CUSTOMER_SERVICE') 
                or (credit_type = 'DOLLAR_AMOUNT' and promotion_id is null and order_type in ('AMAZON','E-COMMERCE')) then '41307 - CARE CONCESSIONS'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'REFERRAL' then '61145 - REFERRAL CREDITS'
            when (credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'BULK_ORDER') 
                or (credit_type = 'DOLLAR_AMOUNT' and promotion_id is null and order_type = 'BULK ORDER') then 'CORPORATE GIFTING'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'GIFT_CARD' then 'GIFT CARD REDEMPTION'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'PROMOTION' then '41301 - NEW CUSTOMER SUBSCRIPTIONS'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'GIFT_CARD_PROMOTION' then '41306 - OTHER'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'RETENTION_OFFER' then '41306 - OTHER'
            when credit_type = 'COW_CASH' and awarded_cow_cash_entry_type = 'REFERRED_CREDIT' then '61145 - REFERRAL CREDITS'
            when credit_type = 'DOLLAR_AMOUNT' and promotion_id in (28,29,30) and promotion_source = 'PROMOTION' then '41301 - NEW CUSTOMER SUBSCRIPTIONS'
            when credit_type = 'DOLLAR_AMOUNT' and promotion_id is null and order_type = 'REPLACEMENT' then 'REPLACEMENTS'
            when credit_type = 'DOLLAR_AMOUNT' and promotion_id is null 
                and order_type in ('MARKETING EVENTS','MARKETING INFLUENCER','PHOTO SHOOTS','PR SAMPLES','WHOLESALE','WHOLESALE SAMPLES') then 'MARKETING PR'
            when credit_type = 'DOLLAR_AMOUNT' and promotion_id is null and order_type = 'GIFT' then 'INTERNAL - HR RELATED'
            when credit_type in ('GIFT_CODE_DOLLAR_AMOUNT','PERCENT_DISCOUNT') and promotion_id = 7 and promotion_source = 'PROMOTION' then '41306 - OTHER'
            when credit_type = 'GIFT_CODE_DOLLAR_AMOUNT' and promotion_id = 10 and promotion_source = 'PROMOTION' then '61145 - REFERRAL CREDITS'
            when credit_type = 'GIFT_CODE_DOLLAR_AMOUNT' and promotion_id = 8 and promotion_source = 'PROMOTION' then '41301 - NEW CUSTOMER SUBSCRIPTIONS'
            else 'OTHER - UNKNOWN'
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
    from add_order_type
)

select * from group_credits
