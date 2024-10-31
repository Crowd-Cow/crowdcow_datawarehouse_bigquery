with
 skus as (select * from {{ ref('skus') }} where dbt_valid_to is null  )
 ,cuts as (select * from {{ ref('cuts') }} where dbt_valid_to is null  )
 
 ,source as (
    SELECT 
        distinct
        cuts.cut_id as CUT_ID
        ,cuts.cut_name as cut_name
        ,skus.category as category
        ,skus.sub_category as sub_category
        ,concat(skus.category,skus.sub_category,cuts.cut_name) as combo
        ,skus.farm_id as farm_id
        ,skus.farm_name as brand
        ,cuts.plu as plu
        --,cuts.plu_weight
        --,batch_size
        --,case_weight
        --,is_primary_vendor
    FROM skus as skus
    LEFT JOIN cuts as cuts ON skus.cut_id = cuts.cut_id
    WHERE 
    skus.is_active_farm
    and cuts.is_in_use
)
SELECT * FROM source