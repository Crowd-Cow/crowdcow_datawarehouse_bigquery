with

flags as ( select * from {{ ref('int_order_flags') }} )

,overall_order_ranks AS (
  SELECT
    order_id,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_created_at_utc) AS overall_order_rank,
    CASE 
      WHEN NOT is_completed_order THEN NULL 
      ELSE SUM(CASE WHEN is_completed_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY order_checkout_completed_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS completed_order_rank,
    CASE 
      WHEN NOT is_paid_order THEN NULL 
      ELSE SUM(CASE WHEN is_paid_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY order_paid_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS paid_order_rank,
    CASE
      WHEN NOT is_cancelled_order THEN NULL
      ELSE SUM(CASE WHEN is_cancelled_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY order_cancelled_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    END AS cancelled_order_rank
  FROM flags
),

ala_carte_order_ranks AS (
  SELECT
    order_id,
    CASE
      WHEN is_membership_order OR NOT is_ala_carte_order THEN NULL
      ELSE SUM(CASE WHEN is_ala_carte_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY order_created_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    END AS ala_carte_order_rank,
    CASE
      WHEN NOT is_completed_order OR NOT is_ala_carte_order OR is_membership_order THEN NULL
      ELSE SUM(CASE WHEN is_completed_order AND is_ala_carte_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY order_checkout_completed_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    END AS completed_ala_carte_order_rank,
    CASE
      WHEN NOT is_paid_order OR is_cancelled_order OR NOT is_ala_carte_order THEN NULL
      ELSE SUM(CASE WHEN is_paid_order AND NOT is_cancelled_order AND is_ala_carte_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY order_paid_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    END AS paid_ala_carte_order_rank,
    CASE
      WHEN NOT is_cancelled_order OR NOT is_ala_carte_order OR is_paid_order THEN NULL
      ELSE SUM(CASE WHEN is_cancelled_order AND is_ala_carte_order AND NOT is_paid_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY order_cancelled_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    END AS cancelled_ala_carte_order_rank
  FROM flags
),

all_memberships_order_ranks AS (
  SELECT
    order_id,
    CASE
      WHEN NOT is_membership_order THEN NULL
      ELSE SUM(CASE WHEN is_membership_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY order_created_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    END AS membership_order_rank,
    CASE
      WHEN NOT is_completed_order OR NOT is_membership_order THEN NULL
      ELSE SUM(CASE WHEN is_completed_order AND is_membership_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY order_checkout_completed_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    END AS completed_membership_order_rank,
    CASE
      WHEN NOT is_paid_order OR is_cancelled_order OR NOT is_membership_order THEN NULL
      ELSE SUM(CASE WHEN is_paid_order AND NOT is_cancelled_order AND is_membership_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY order_paid_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    END AS paid_membership_order_rank,
    CASE
      WHEN NOT is_cancelled_order OR is_paid_order OR NOT is_membership_order THEN NULL
      ELSE SUM(CASE WHEN is_cancelled_order AND is_membership_order AND NOT is_paid_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY order_cancelled_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    END AS cancelled_membership_order_rank
  FROM flags
),

unique_memberships_order_ranks AS (
  SELECT
    order_id,
    CASE
      WHEN NOT is_membership_order THEN NULL
      ELSE SUM(CASE WHEN is_membership_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id, subscription_id ORDER BY order_created_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    END AS unique_membership_order_rank,
    CASE
      WHEN NOT is_completed_order OR NOT is_membership_order THEN NULL
      ELSE SUM(CASE WHEN is_completed_order AND is_membership_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id, subscription_id ORDER BY order_checkout_completed_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    END AS completed_unique_membership_order_rank,
    CASE
      WHEN NOT is_paid_order OR is_cancelled_order OR NOT is_membership_order THEN NULL
      ELSE SUM(CASE WHEN is_paid_order AND is_membership_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id, subscription_id ORDER BY order_paid_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    END AS paid_unique_membership_order_rank,
    CASE
      WHEN NOT is_cancelled_order OR is_paid_order OR NOT is_membership_order THEN NULL
      ELSE SUM(CASE WHEN is_cancelled_order AND is_membership_order AND NOT is_paid_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id, subscription_id ORDER BY order_cancelled_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    END AS cancelled_unique_membership_order_rank
  FROM flags
)

,gift_order_ranks as (
  select
    order_id

      ,CASE
          WHEN NOT is_gift_order THEN NULL
          ELSE SUM(CASE WHEN is_gift_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY order_created_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      END AS gift_order_rank

      ,CASE
          WHEN NOT is_gift_order OR NOT is_completed_order THEN NULL
          ELSE SUM(CASE WHEN is_gift_order AND is_completed_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY order_checkout_completed_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      END AS completed_gift_order_rank

      ,CASE
          WHEN NOT is_gift_order OR NOT is_paid_order OR is_cancelled_order THEN NULL
          ELSE SUM(CASE WHEN is_gift_order AND is_paid_order AND NOT is_cancelled_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY order_paid_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      END AS paid_gift_order_rank

      ,CASE
          WHEN NOT is_gift_order OR NOT is_cancelled_order OR is_paid_order THEN NULL
          ELSE SUM(CASE WHEN is_gift_order AND is_cancelled_order AND NOT is_paid_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY order_cancelled_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      END AS cancelled_gift_order_rank
  from flags

)

,gift_card_order_ranks as (
    select 
      order_id

      ,CASE 
          WHEN NOT is_gift_card_order THEN NULL
          ELSE SUM(CASE WHEN is_gift_card_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY order_created_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      END AS gift_card_order_rank

      ,CASE
          WHEN NOT is_gift_card_order OR NOT is_completed_order THEN NULL
          ELSE SUM(CASE WHEN is_gift_card_order AND is_completed_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY order_checkout_completed_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      END AS completed_gift_card_order_rank

      ,CASE
          WHEN NOT is_gift_card_order OR NOT is_paid_order OR is_cancelled_order THEN NULL
          ELSE SUM(CASE WHEN is_gift_card_order AND is_paid_order AND NOT is_cancelled_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY order_paid_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      END AS paid_gift_card_order_rank

      ,CASE
          WHEN NOT is_gift_card_order OR NOT is_cancelled_order OR is_paid_order THEN NULL
          ELSE SUM(CASE WHEN is_gift_card_order AND is_cancelled_order AND NOT is_paid_order THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY order_cancelled_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      END AS cancelled_gift_card_order_rank

    from flags
)

,moolah_order_ranks as (
    select 
      order_id

     ,CASE 
          WHEN NOT is_moolah_order THEN NULL
          ELSE COUNTIF(is_moolah_order) OVER (PARTITION BY user_id ORDER BY order_created_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      END AS moolah_order_rank

      ,CASE
          WHEN NOT is_moolah_order OR NOT is_completed_order THEN NULL
          ELSE COUNTIF(is_moolah_order AND is_completed_order) OVER (PARTITION BY user_id ORDER BY order_checkout_completed_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      END AS completed_moolah_order_rank

      ,CASE
          WHEN NOT is_moolah_order OR NOT is_paid_order OR is_cancelled_order THEN NULL
          ELSE COUNTIF(is_moolah_order AND is_paid_order AND NOT is_cancelled_order) OVER (PARTITION BY user_id ORDER BY order_paid_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      END AS paid_moolah_order_rank

      ,CASE
          WHEN NOT is_moolah_order OR NOT is_cancelled_order OR is_paid_order THEN NULL
          ELSE COUNTIF(is_moolah_order AND is_cancelled_order AND NOT is_paid_order) OVER (PARTITION BY user_id ORDER BY order_cancelled_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      END AS cancelled_moolah_order_rank

    from flags 
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
      ,gift_card_order_ranks.cancelled_gift_card_order_rank
      ,moolah_order_ranks.moolah_order_rank
      ,moolah_order_ranks.completed_moolah_order_rank
      ,moolah_order_ranks.paid_moolah_order_rank
      ,moolah_order_ranks.cancelled_moolah_order_rank
    from flags
      left join overall_order_ranks on flags.order_id = overall_order_ranks.order_id
      left join ala_carte_order_ranks on flags.order_id = ala_carte_order_ranks.order_id
      left join all_memberships_order_ranks on flags.order_id = all_memberships_order_ranks.order_id
      left join unique_memberships_order_ranks on flags.order_id = unique_memberships_order_ranks.order_id
      left join gift_order_ranks on flags.order_id = gift_order_ranks.order_id
      left join gift_card_order_ranks on flags.order_id = gift_card_order_ranks.order_id
      left join moolah_order_ranks on flags.order_id = moolah_order_ranks.order_id

)

select * from combine_ranks
