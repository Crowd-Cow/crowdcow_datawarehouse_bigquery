with

source as ( select * from {{ source('cc', 'promotions_subscription_enrollments') }} where  __deleted is null and id not in (8167, 8416))
,promotions_promotions as ( select * from {{ ref('stg_cc__promotions_promotions')  }})

,final as (
    select 
        source.id aspromotions_subscription_enrollments_id,
        source.promotions_promotion_id,
        source.subscription_id,
        source.created_at as created_at_utc,
        promotions_promotions.name as promotion_name,
        promotions_promotions.notes as promotion_notes,
        ROW_NUMBER() over (PARTITION BY source.subscription_id order by source.updated_at desc ) as rn
    from source 
    left join promotions_promotions on promotions_promotions.id = source.promotions_promotion_id

)

select * from final where rn = 1 

