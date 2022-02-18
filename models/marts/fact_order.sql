{{
  config(
        enabled = false
    )
}}

with 

orders as ( select * from {{ ref('stg_cc__orders') }} )
, gift_flag as ( select distinct order_id from {{ ref('stg_cc__gift_infos') }} )

, join_gift_flag as (
    select
        orders.*
        ,gift_flag.order_id is not null as is_gift_order
    from orders
        left join gift_flag on orders.order_id = gift_flag.order_id
)

, order_rank as (
    select user_id
        , order_paid_at_utc
        , order_id
        , rank () over (partition by user_id order by order_paid_at_utc, order_id ) as all_paid_order_rank
    from join_gift_flag 
    where order_cancelled_at_utc is null 
        and order_paid_at_utc is not null 
        and stripe_failure_code is null 
)

, subscription_rank as ( 
    select user_id
        , order_paid_at_utc
        , order_id
        , rank () over (partition by user_id order by order_paid_at_utc, order_id ) as subscription_paid_order_rank
    from join_gift_flag 
    where order_cancelled_at_utc is null 
        and order_paid_at_utc is not null 
        and stripe_failure_code is null
        and subscription_id is not null
)

, alacarte_rank as ( 
    select user_id
        , order_paid_at_utc
        , order_id
        , rank () over (partition by user_id order by order_paid_at_utc, order_id ) as alc_paid_order_rank
    from join_gift_flag 
    where order_cancelled_at_utc is null 
        and order_paid_at_utc is not null 
        and stripe_failure_code is null
        and subscription_id is null
)

, gift_order_rank as (
    select
        order_id
        ,rank() over(partition by user_id order by order_paid_at_utc, order_id) as gift_paid_order_rank
    from join_gift_flag
    where order_cancelled_at_utc is null
        and order_paid_at_utc is not null
        and stripe_failure_code is null
        and is_gift_order
)

, all_up_order_ranks as ( 
    select join_gift_flag.* 
        , order_rank.all_paid_order_rank
        , subscription_rank.subscription_paid_order_rank
        , alacarte_rank.alc_paid_order_rank
        , gift_order_rank.gift_paid_order_rank
    from join_gift_flag
    left join order_rank on join_gift_flag.order_id = order_rank.order_id
    left join subscription_rank on join_gift_flag.order_id = subscription_rank.order_id
    left join alacarte_rank on join_gift_flag.order_id = alacarte_rank.order_id
    left join gift_order_rank on join_gift_flag.order_id = gift_order_rank.order_id
)

select * from all_up_order_ranks
