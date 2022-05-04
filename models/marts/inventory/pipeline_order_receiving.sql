with

ordered_items as ( select * from {{ ref('pipeline_receivables') }} where not is_destroyed )
,received_items as ( select * from {{ ref('sku_lots') }} )
,current_sku as ( select * from {{ ref('skus') }} where dbt_valid_to is null )

