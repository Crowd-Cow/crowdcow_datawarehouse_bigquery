with

credit as ( select * from {{ ref('stg_cc__credits') }} )
,cow_cash as ( select * from {{ ref('stg_cc__cow_cash_entries') }} )
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
        left join cow_cash as awarded_cow_cash on credit.cow_cash_entry_source_id = awarded_cow_cash.cow_cash_id
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

,group_credit_descriptions as (
    select
        credit_id
        ,promotion_id
        ,user_id
        ,order_id
        ,cow_cash_entry_source_id
        ,credit_type
        ,credit_description

        ,case
            when (credit_description like '%LATE%' and credit_description not like '%RELATED%')
                or credit_description like any ('%DELAY%','%ARRIVED LATE%','%DELIVERY FAILURE%') then 'LATE SHIPMENT'
            when credit_description like any ('%LOST%','%MISSING ORDER%','%NEVER ARRIVE%','%UNDELIVER%','%NON-DELIVER%') then 'LOST ORDER'
            when credit_description like '%THAW%' then 'THAWED'
            when credit_description like any ('%INVENTORY%','%INR%','%OOS%','%MISSING_ITEM%','%ITEM MISSING%','%MISSING FROM%') then  'MISSING ITEM'
            when credit_description like '%LEAK%' then 'LEAKER'
            when credit_description = 'REPLACEMENT'
                or credit_description like any ('REPLACEMENT ORDER%','REPLACEMENT 2%','REPLACEMENT FOR%') then 'REPLACEMENT'
            when credit_description like any ('%NEW SUB%','%FIRST SUB%','%1ST SUB%','%SUBSCRIPTION PRICE%','%SUBSCRIBER DISCOUNT%','%SUBSCRIPTION DISCOUNT%'
                ,'%MEMBER DISCOUNT%','%MEMBERSHIP DISCOUNT%','%MEMBER PRICING%','%MEMBER PRICE%','%MEMBERSHIP PRICING%','%MEMBERSHIP PRICE%','%SUBSCRIPTION PRICING%') then 'MEMBER PRICE FIX'
            when credit_description like any ('%GB PROMO%','%BACON PROMO%','%HONORING PROMO%','%BEEF PROMO%','%WAGYU PROMO%','%FREE PROMO%') then 'PRODUCT PROMO'
            when credit_description like '%QUALITY%' then 'QUALITY ISSUE'
            when credit_description like '%MISSING_PROMO%' then 'MISSING PROMO'
            when credit_description like '%MATCH%' then 'PRICE MATCH'
            when credit_description like any ('%FEDEX%','%DAMAGE%','%AXLEHIRE%') then 'CARRIER ISSUE'
            when credit_description like any ('%ACCIDENTAL SUBSCRIPTION%','%UNINTENDED SUBSCRIPTION%','%NOT WANT SUB%') then 'DID NOT WANT MEMBERSHIP'
            when credit_description like any ('%MARKETING%','%INFLUENCER%','%AMBASSADOR%') then 'MARKETING'
            when credit_description like '%WHOLESALE%' then 'WHOLESALE DISCOUNT'
         end as credit_group
         
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

select * from group_credit_descriptions
