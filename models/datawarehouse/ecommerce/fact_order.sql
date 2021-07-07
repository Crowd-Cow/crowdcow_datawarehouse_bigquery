with stage as (

    select * from {{ ref('stg_cc__orders') }}

)

, order_rank as (
    select user_id
        , order_paid_at_utc
        , order_id
        , rank () over (partition by user_id order by order_paid_at_utc, order_id ) as all_paid_order_rank
    from stage 
    where order_cancelled_at_utc is null 
        and order_paid_at_utc is not null 
        and stripe_failure_code is null 
)
, subscription_rank as ( 
    select user_id
        , order_paid_at_utc
        , order_id
        , rank () over (partition by user_id order by order_paid_at_utc, order_id ) as subscription_paid_order_rank
    from stage 
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
    from stage 
    where order_cancelled_at_utc is null 
        and order_paid_at_utc is not null 
        and stripe_failure_code is null
        and subscription_id is null
)
, all_up_order_ranks as ( 
    select stage.* 
        , order_rank.all_paid_order_rank
        , subscription_rank.subscription_paid_order_rank
        , alacarte_rank.alc_paid_order_rank
    from stage
    left join order_rank on order_rank.order_id = stage.order_id 
    left join subscription_rank on subscription_rank.order_id = stage.order_id
    left join alacarte_rank on alacarte_rank.order_id = stage.order_id
)

select * from all_up_order_ranks 
