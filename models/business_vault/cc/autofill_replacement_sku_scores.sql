with

current_replacement_score as ( select * From {{ ref('stg_cc__autofill_replacement_sku_scores') }} where dbt_valid_to is null)

select
    autofilL_replacement_score_id
    ,autofill_replacement_score_key
    ,sku_id
    ,replacement_sku_id
    ,replacement_score
    ,created_at_utc
    ,updated_at_utc
from current_replacement_score
