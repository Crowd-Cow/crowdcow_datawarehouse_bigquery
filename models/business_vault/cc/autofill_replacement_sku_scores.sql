with

current_replacement_score as ( select * From {{ ref('stg_cc__autofill_replacement_sku_scores') }} where dbt_valid_to is null)
,sku as ( select * from {{ ref('skus') }} )

,get_sku_keys as (
    select
        current_replacement_score.autofilL_replacement_score_id
        ,current_replacement_score.autofill_replacement_score_key
        ,current_replacement_score.sku_id
        ,sku.sku_key
        ,current_replacement_score.replacement_sku_id
        ,replacement_sku.sku_key as replacement_sku_key
        ,current_replacement_score.replacement_score
        ,current_replacement_score.created_at_utc
        ,current_replacement_score.updated_at_utc
    from current_replacement_score
        left join sku on current_replacement_score.sku_id = sku.sku_id
            and current_replacement_score.updated_at_utc >= sku.adjusted_dbt_valid_from
            and current_replacement_score.updated_at_utc < sku.adjusted_dbt_valid_to
        left join sku as replacement_sku on current_replacement_score.replacement_sku_id = replacement_sku.sku_id
            and current_replacement_score.updated_at_utc >= replacement_sku.adjusted_dbt_valid_from
            and current_replacement_score.updated_at_utc < replacement_sku.adjusted_dbt_valid_to
)

select * from get_sku_keys
