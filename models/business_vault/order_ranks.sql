with

flags as ( select * from {{ ref('order_flags') }} )

,ranks as (
    select
        order_id
        ,user_id
        ,subscription_id
        ,order_created_at_utc
        ,row_number() over(partition by user_id order by order_created_at_utc) as overall_order_rank   

        ,case 
            when not is_completed_order then null 
            else conditional_true_event(is_completed_order) over (partition by user_id order by order_created_at_utc) 
         end as completed_order_rank

        ,case 
            when not is_paid_order then null 
            else conditional_true_event(is_paid_order) over (partition by user_id order by order_created_at_utc) 
         end as paid_order_rank

        ,case
            when not is_cancelled_order then null
            else conditional_true_event(is_cancelled_order) over (partition by user_id order by order_created_at_utc) 
         end as cancelled_order_rank

        ,case
            when not is_membership_order then null
            else conditional_true_event(is_membership_order) over(partition by user_id order by order_created_at_utc)
          end as membership_order_rank

         ,case
            when is_membership_order then null
            else conditional_true_event(is_ala_carte_order) over(partition by user_id order by order_created_at_utc)
          end as ala_carte_order_rank

        ,case
            when not is_paid_order or is_cancelled_order or not is_membership_order then null
            else conditional_true_event(is_paid_order and is_membership_order) over (partition by user_id order by order_created_at_utc) 
         end as paid_membership_order_rank

         ,case
            when not is_paid_order or is_cancelled_order or not is_ala_carte_order then null
            else conditional_true_event(is_paid_order and not is_cancelled_order and is_ala_carte_order) over (partition by user_id order by order_created_at_utc)
          end as paid_ala_carte_order_rank

    from flags
)

select *
from ranks