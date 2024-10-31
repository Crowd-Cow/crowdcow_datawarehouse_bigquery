with
 skus as (select * from {{ ref('skus') }} where dbt_valid_to is null  )
 ,cuts as (select * from {{ ref('cuts') }} where dbt_valid_to is null  )
 ,inventory_classification AS (
        SELECT 
            DISTINCT
            {{ dbt_utils.surrogate_key(['skus.category','skus.sub_category','cuts.cut_name']) }} as inventory_classification_id,
            skus.category,
            skus.sub_category,
            cuts.cut_name,
            CONCAT(skus.category, skus.sub_category, cuts.cut_name) AS combo,
            --,skus.inventory_classification,
            skus.replenishment_code
        FROM skus
        LEFT JOIN cuts ON skus.cut_id = cuts.cut_id
        WHERE 
            skus.is_active_farm = true
            AND cuts.is_in_use = true 

    )
    SELECT 
        *
    FROM inventory_classification