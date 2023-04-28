with
    inventory as ( select * from {{ ref('stg_gs__inventory_classification_master_v_2') }} )

SELECT *
FROM inventory