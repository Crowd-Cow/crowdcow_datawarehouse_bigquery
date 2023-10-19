with

promotions_configurations as ( select * from {{ ref('stg_cc__promotions_configurations') }}),
promotions_effects as ( select * from {{ ref('stg_cc__promotions_effects') }}),
promotions_promotions as ( select * from {{ ref('stg_cc__promotions_promotions') }})

,promotion_ids AS (
    SELECT
        promotions_promotions.id
    FROM
        promotions_configurations
        INNER JOIN promotions_promotions ON promotions_promotions.id = promotions_configurations.configurable_id
    WHERE
        promotions_configurations.configurable_type = 'PROMOTIONS::PROMOTION'
        AND promotions_configurations.promotion_configuration_key = 'REWARDS_PROGRAM'
        AND promotions_configurations.promotion_configuration_value = 'MOOLAH'
),
reward_ids AS (
    SELECT
        promotions_effects.id
    FROM
        promotions_effects
        INNER JOIN promotion_ids ON promotion_ids.id = promotions_effects.promotions_promotion_id
),
quantity as (
    SELECT
        promotions_configurations.configurable_id,
        promotions_configurations.promotion_configuration_value
    FROM
        promotions_configurations
    WHERE
        promotions_configurations.promotion_configuration_key = 'QUANTITY'
)

,promo_configurations AS (
    SELECT
        promotions_configurations.promotion_configuration_id,
        promotions_configurations.promotion_configuration_key,
        promotions_configurations.created_at_utc,
        promotions_configurations.promotion_configuration_value AS sku_id,
        promotions_configurations.configurable_type,
        promotions_configurations.configurable_id,
        promotions_configurations.updated_at_utc,
        quantity.promotion_configuration_value as quantity
    FROM
        promotions_configurations
        INNER JOIN reward_ids ON reward_ids.id = promotions_configurations.configurable_id
        LEFT JOIN quantity ON quantity.configurable_id = promotions_configurations.configurable_id
    WHERE
        promotions_configurations.promotion_configuration_key = 'SKU_IDS'
        AND promotions_configurations.configurable_type = 'PROMOTIONS::REWARD'
)

SELECT * FROM promo_configurations