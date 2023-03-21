with

promotions_configurations as ( select * from {{ ref('stg_cc__promotions_configurations') }}),
promotions_rewards as ( select * from {{ ref('stg_cc__promotions_rewards') }}),
promotions_promotions as ( select * from {{ ref('stg_cc__promotions_promotions') }})

,current_moolah_skus as (
SELECT 
    promotions_configurations.promotion_configuration_id,
    promotions_configurations.promotion_configuration_key,
    promotions_configurations.created_at_utc,
    promotions_configurations.promotion_configuration_value as sku_id,
    promotions_configurations.configurable_type,
    promotions_configurations.configurable_id,
    promotions_configurations.updated_at_utc
FROM promotions_configurations 
WHERE 
    promotions_configurations.promotion_configuration_key IN ('SKU_IDS', 'QUANTITY') 
    AND promotions_configurations.configurable_type = 'PROMOTIONS::REWARD' 
    AND promotions_configurations.configurable_id 
    IN (SELECT 
        promotions_rewards.id 
        FROM promotions_rewards 
        WHERE 
            promotions_rewards.promotions_promotion_id 
            IN (SELECT 
                promotions_promotions.id 
                FROM promotions_promotions 
                WHERE 
                promotions_promotions.id 
                IN (SELECT 
                promotions_configurations.configurable_id 
                FROM promotions_configurations 
                WHERE 
                promotions_configurations.configurable_type = 'PROMOTIONS::PROMOTION' 
                AND promotions_configurations.promotion_configuration_key = 'REWARDS_PROGRAM' 
                AND promotions_configurations.promotion_configuration_value = 'MOOLAH')
            )
        )

)

SELECT * FROM current_moolah_skus