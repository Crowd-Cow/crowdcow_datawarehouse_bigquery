with

flags as ( select * from {{ ref('order_flags') }} )

,overall_order_ranks as(
  select
    order_id
    ,row_number() over(partition by user_id order_by order_created_at_utc) as overall_order_rank
    
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

    from flags
)

,ala_carte_order_ranks as (
    select
      order_id

      ,case
        when is_membership_order or not is_ala_carte_order then null
        else conditional_true_event(is_ala_carte_order) over(partition by user_id order by order_created_at_utc)
       end as ala_carte_order_rank

      ,case
        when not is_completed_order or not is_ala_carte_order or is_membership_order then null
        else conditional_true_event(is_completed_order and is_ala_carte_order) over (partition by user_id order by order_created_at_utc)
       end as completed_ala_carte_order_rank

      ,case
        when not is_paid_order or is_cancelled_order or not is_ala_carte_order then null
        else conditional_true_event(is_paid_order and not is_cancelled_order and is_ala_carte_order) over (partition by user_id order by order_created_at_utc)
       end as paid_ala_carte_order_rank

      ,case
        when not is_cancelled_order or not is_ala_carte_order or is_paid_order then null
        else conditional_true_event(is_cancelled_order and is_ala_carte_order and not is_paid_order) over(partition by user_id order by order_created_at_utc)
       end as cancelled_ala_carte_order_rank

    from flags
)

,all_memberships_order_ranks as (
  select
    order_id
    
    ,case
      when not is_membership_order then null
      else conditional_true_event(is_membership_order) over(partition by user_id order by order_created_at_utc)
     end as membership_order_rank

    ,case
      when not is_completed_order or not is_membership_order then null
      else conditional_true_event(is_completed_order and is_membership_order) over (partition by user_id order by order_created_at_utc)
     end as completed_membership_order_rank

    ,case
      when not is_paid_order or is_cancelled_order or not is_membership_order then null
      else conditional_true_event(is_paid_order and not is_cancelled_order and is_membership_order) over (partition by user_id order by order_created_at_utc) 
     end as paid_membership_order_rank

    ,case
      when not is_cancelled_order or is_paid_order or not is_membership_order then null
      else conditional_true_event(is_cancelled_order and is_membership_order and not is_paid_order) over (partition by user_id order by order_created_at_utc)
     end as cancelled_membership_order_rank

  from flags
)

,unique_memberships_order_ranks as (
  select
    order_id

    ,case
      when not is_membership_order then null
      else conditional_true_event(is_membership_order) over(partition by user_id, subscription_id order by order_created_at_utc)
     end as unique_membership_order_rank

    ,case
      when not is_completed_order or not is_membership_order then null
      else conditional_true_event(is_completed_order and is_membership_order) over (partition by user_id, subscription_id order by order_created_at_utc)
     end as completed_unique_membership_order_rank

    ,case
      when not is_paid_order or is_cancelled_order or not is_membership_order then null
      else conditional_true_event(is_paid_order and is_membership_order) over (partition by user_id, subscription_id order by order_created_at_utc) 
     end as paid_unique_membership_order_rank

    ,case
      when not is_cancelled_order or is_paid_order or not is_membership_order then null
      else conditional_true_event(is_cancelled_order and is_membership_order and not is_paid_order) over(partition by user_id, subscription_id order by order_created_at_utc)
     end as cancelled_unique_membership_order_rank

  from flags

)

,gift_order_ranks as (
  select
    order_id

    ,case
      when not is_gift_order then null
      else conditional_true_event(is_gift_order) over (partition by user_id order by order_created_at_utc)
     end as gift_order_rank

    ,case
      when not is_gift_order or not is_completed_order then null
      else conditional_true_event(is_gift_order and is_completed_order) over(partition by user_id order by order_created_at_utc)
     end as completed_gift_order_rank

    ,case
      when not is_gift_order or not is_paid_order or is_cancelled_order then null
      else conditional_true_event(is_gift_order and is_paid_order and not is_cancelled_order) over (partition by user_id order by order_created_at_utc)
     end as paid_gift_order_rank

    ,case
      when not is_gift_order or not is_cancelled_order or is_paid_order then null
      else conditional_true_event(is_gift_order and is_cancelled_order and not is_paid_order) over(partition by user_id order by order_created_at_utc)
     end as cancelled_gift_order_rank

  from flags

)

,gift_card_order_ranks as (
    select 
      order_id

      ,case 
        when not is_gift_card_order then null
        else conditional_true_event(is_gift_card_order) over (partition by user_id order by order_created_at_utc)
       end as gift_card_order_rank

      ,case
        when not is_gift_card_order or not is_completed_order then null
        else conditional_true_event(is_gift_card_order and is_completed_order) over(partition by user_id order by order_created_at_utc)
       end as completed_gift_card_order_rank

      ,case
        when not is_gift_card_order or not is_paid_order or is_cancelled_order then null
        else conditional_true_event(is_gift_card_order and is_paid_order and not is_cancelled_order) over (partition by user_id order by order_created_at_utc)
       end as paid_gift_card_order_rank

      ,case
        when not is_gift_card_order or not is_cancelled_order or is_paid_order then null
        else conditional_true_event(is_gift_card_order and is_cancelled_order and not is_paid_order) over(partition by user_id order by order_created_at_utc)
       end as cancelled_gift_card_order
)

,combine_ranks as (
    select
      flags.order_id
      ,overall_order_ranks.overall_order_rank
      ,overall_order_ranks.completed_order_rank
      ,overall_order_ranks.paid_order_rank
      ,overall_order_ranks.cancelled_order_rank
      ,ala_carte_order_ranks.ala_carte_order_rank
      ,ala_carte_order_ranks.completed_ala_carte_order_rank
      ,ala_carte_order_ranks.paid_ala_carte_order_rank
      ,ala_carte_order_ranks.cancelled_ala_carte_order_rank
      ,all_memberships_order_ranks.membership_order_rank
      ,all_memberships_order_ranks.completed_membership_order_rank
      ,all_memberships_order_ranks.paid_membership_order_rank
      ,all_memberships_order_ranks.cancelled_membership_order_rank
      ,unique_memberships_order_ranks.unique_membership_order_rank
      ,unique_memberships_order_ranks.completed_unique_membership_order_rank
      ,unique_memberships_order_ranks.paid_unique_membership_order_rank
      ,unique_memberships_order_ranks.cancelled_unique_membership_order_rank
      ,gift_order_ranks.gift_order_rank
      ,gift_order_ranks.completed_gift_order_rank
      ,gift_order_ranks.paid_gift_order_rank
      ,gift_order_ranks.cancelled_gift_order_rank
      ,gift_card_order_ranks.gift_card_order_rank
      ,gift_card_order_ranks.completed_gift_card_order_rank
      ,gift_card_order_ranks.paid_gift_card_order_rank
      ,gift_card_order_ranks.cancelled_gift_card_order
    from flags
      left join overall_order_ranks on flags.order_id = overall_order_ranks.order_id
      left join ala_carte_order_ranks on flags.order_id = ala_carte_order_ranks.order_id
      left join all_memberships_order_ranks on flags.order_id = all_memberships_order_ranks.order_id
      left join unique_memberships_order_ranks on flags.order_id = unique_memberships_order_ranks.order_id
      left join gift_order_ranks on flags.order_id = gift_order_ranks.order_id
      left join gift_card_order_ranks on flags.order_id = gift_card_order_ranks.order_id
)

select * from combine_ranks
