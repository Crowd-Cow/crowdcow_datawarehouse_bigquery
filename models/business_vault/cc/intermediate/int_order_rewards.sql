with

reward as ( select * from {{ ref('stg_cc__reward_points') }} )

select
    order_id
    ,sum(iff(rewards_program = 'WAGYU_CLUB',reward_spend_amount,0)) as jwagyu_reward_spend
from reward
group by 1
