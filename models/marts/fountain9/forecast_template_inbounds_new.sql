with

PIPELINE_RECEIVABLES as ( select * from {{ ref('pipeline_receivables') }} )
,SKUS as ( select * from {{ ref('skus') }} )
,FCS as ( select * from {{ ref('fcs') }} )


SELECT
    pipeline_receivables.RECEIVABLE_STATUS  AS receivable_status,
    pipeline_receivables.FARM_OUT_NAME  AS pipeline_receivables_farm_name,
    skus.sku_name  AS sku_name,
    skus.CATEGORY  AS category,
    skus.SUB_CATEGORY  AS sub_category,
    fcs.FC_NAME  AS fc_name,
    skus.FARM_NAME  AS farm_name,
        (TO_CHAR(TO_DATE(DATEADD('day', (0 - EXTRACT(DOW FROM CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(pipeline_receivables.FC_SCAN_PROPOSED_DATE  AS TIMESTAMP_NTZ)))::integer), CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(pipeline_receivables.FC_SCAN_PROPOSED_DATE  AS TIMESTAMP_NTZ)))), 'YYYY-MM-DD')) AS fc_scan_proposed_week,
    pipeline_receivables.LOT_NUMBER  AS lot_number,
        (TO_CHAR(DATE_TRUNC('month', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(pipeline_receivables.FC_SCAN_PROPOSED_DATE  AS TIMESTAMP_NTZ))), 'YYYY-MM')) AS fc_scan_proposed_month,
    COALESCE(SUM(pipeline_receivables.quantity_ordered ), 0) AS quantity_ordered,
    COALESCE(SUM(CASE WHEN (CASE WHEN pipeline_receivables.IS_DESTROYED  THEN 1 ELSE 0 END
) = 0 THEN pipeline_receivables.quantity_ordered  ELSE NULL END), 0) AS non_destroyed_receivables
FROM PIPELINE_RECEIVABLES
     AS pipeline_receivables
LEFT JOIN SKUS
     AS skus ON pipeline_receivables.sku_key = skus.sku_key
LEFT JOIN FCS
     AS fcs ON (pipeline_receivables.FC_ID) = (fcs.FC_ID)
      and (TO_CHAR(TO_DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(fcs.DBT_VALID_TO  AS TIMESTAMP_NTZ))), 'YYYY-MM-DD')) is null
WHERE (pipeline_receivables.FC_SCAN_PROPOSED_DATE ) >= ((CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', CAST(DATEADD('day', -91, TO_DATE(DATEADD('day', (0 - EXTRACT(DOW FROM DATE_TRUNC('day', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ))))::integer), DATE_TRUNC('day', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ)))))) AS TIMESTAMP_NTZ)))) AND ((not pipeline_receivables.is_rastellis or pipeline_receivables.is_rastellis is null) )
GROUP BY
    (TO_DATE(DATEADD('day', (0 - EXTRACT(DOW FROM CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(pipeline_receivables.FC_SCAN_PROPOSED_DATE  AS TIMESTAMP_NTZ)))::integer), CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(pipeline_receivables.FC_SCAN_PROPOSED_DATE  AS TIMESTAMP_NTZ))))),
    (DATE_TRUNC('month', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(pipeline_receivables.FC_SCAN_PROPOSED_DATE  AS TIMESTAMP_NTZ)))),
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    9
ORDER BY
    8 DESC 
